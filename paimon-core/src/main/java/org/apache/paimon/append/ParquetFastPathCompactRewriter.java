/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetRowGroupCopier;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.format.parquet.ParquetSimpleStatsExtractor;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics.MissReason;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.stats.SimpleStatsMerger;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.org.apache.parquet.column.Encoding;
import org.apache.paimon.shade.org.apache.parquet.column.ParquetProperties;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.paimon.shade.org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.paimon.shade.org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.StatsCollectorFactories.createStatsFactories;

/** Fast-path rewriter that concatenates Parquet RowGroups for append-only compaction. */
public class ParquetFastPathCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFastPathCompactRewriter.class);

    private ParquetFastPathCompactRewriter() {}

    @Nullable
    public static List<DataFileMeta> tryRewrite(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType writeType,
            CoreOptions options,
            BinaryRow partition,
            int bucket,
            @Nullable Function<String, DeletionVector> dvFactory,
            List<DataFileMeta> toCompact,
            DataFilePathFactory pathFactory,
            long schemaId,
            @Nullable CompactionFastPathMetrics metrics) {
        long startNanos = System.nanoTime();
        long inputBytes = toCompact.stream().mapToLong(DataFileMeta::fileSize).sum();
        long inputRows = toCompact.stream().mapToLong(DataFileMeta::rowCount).sum();
        try {
            if (dvFactory != null) {
                reportMiss(
                        metrics,
                        MissReason.DV,
                        String.format(
                                "inputFiles=%d, inputRows=%d, inputBytes=%d",
                                toCompact.size(), inputRows, inputBytes));
                return null;
            }

            MessageType expectedSchema =
                    ParquetSchemaConverter.convertToParquetMessageType(writeType);
            String expectedCodec = normalizeCompression(options.fileCompression());
            List<String> expectedValueStatsCols = toCompact.get(0).valueStatsCols();

            MissReason tableMiss =
                    checkTableLevel(options, fileFormat, bucket, expectedValueStatsCols, toCompact);
            if (tableMiss != null) {
                reportMiss(
                        metrics,
                        tableMiss,
                        String.format(
                                "bucket=%d, fileFormat=%s, inputFiles=%d",
                                bucket, options.fileFormatString(), toCompact.size()));
                return null;
            }

            List<PreparedInput> preparedInputs = new ArrayList<>(toCompact.size());
            for (int i = 0; i < toCompact.size(); i++) {
                DataFileMeta file = toCompact.get(i);
                MissReason fileMiss = checkFileLevel(file, schemaId, expectedValueStatsCols);
                if (fileMiss != null) {
                    reportMiss(
                            metrics,
                            fileMiss,
                            String.format(
                                    "file=%s, fileIndex=%d, schemaId=%d",
                                    file.fileName(), i, file.schemaId()));
                    return null;
                }
                Path filePath = pathFactory.toPath(file);
                ParquetInputFile inputFile =
                        ParquetInputFile.fromPath(fileIO, filePath, file.fileSize());
                try (ParquetFileReader reader =
                        ParquetUtil.getParquetReader(
                                fileIO, filePath, file.fileSize(), options.toConfiguration())) {
                    ParquetMetadata footer = reader.getFooter();
                    MissReason footerMiss =
                            checkFooterLevel(
                                    footer,
                                    expectedSchema,
                                    expectedCodec,
                                    preparedInputs.isEmpty() ? null : preparedInputs.get(0).codec);
                    if (footerMiss != null) {
                        reportMiss(
                                metrics,
                                footerMiss,
                                String.format(
                                        "file=%s, fileIndex=%d, expectedCodec=%s",
                                        file.fileName(), i, expectedCodec));
                        return null;
                    }
                    CompressionCodecName codec =
                            footer.getBlocks().isEmpty()
                                    ? CompressionCodecName.UNCOMPRESSED
                                    : footer.getBlocks().get(0).getColumns().get(0).getCodec();
                    preparedInputs.add(new PreparedInput(file, inputFile, footer, codec));
                }
            }

            long prepareMs = elapsedMillis(startNanos);
            int inputRowGroups =
                    preparedInputs.stream().mapToInt(input -> input.blocks.size()).sum();
            List<ParquetRowGroupCopier.Input> copierInputs = new ArrayList<>(preparedInputs.size());
            for (PreparedInput preparedInput : preparedInputs) {
                copierInputs.add(
                        new ParquetRowGroupCopier.Input(
                                preparedInput.inputFile, preparedInput.metadata));
            }

            ParquetRowGroupCopier copier =
                    new ParquetRowGroupCopier(
                            fileIO,
                            expectedSchema,
                            options.targetFileSize(false),
                            pathFactory::newPath,
                            options.toConfiguration(),
                            options.appendCompactionRowGroupCopyPreservePageIndex());
            long copyStartNanos = System.nanoTime();
            List<ParquetRowGroupCopier.OutputFile> copiedFiles = copier.copy(copierInputs);
            long copyMs = elapsedMillis(copyStartNanos);
            try {
                long buildStartNanos = System.nanoTime();
                List<DataFileMeta> result =
                        buildResult(
                                copiedFiles,
                                preparedInputs,
                                toCompact,
                                writeType,
                                schemaId,
                                expectedValueStatsCols,
                                options,
                                pathFactory);
                long buildResultMs = elapsedMillis(buildStartNanos);
                long outputBytes = result.stream().mapToLong(DataFileMeta::fileSize).sum();
                long outputRows = result.stream().mapToLong(DataFileMeta::rowCount).sum();
                if (metrics != null) {
                    metrics.reportHit();
                }
                LOG.info(
                        "Append compaction fast path succeeded: inputFiles={}, inputRows={}, "
                                + "inputBytes={}, inputRowGroups={}, outputFiles={}, outputRows={}, "
                                + "outputBytes={}, prepareMs={}, copyMs={}, buildResultMs={}, "
                                + "totalMs={}, preservePageIndex={}",
                        toCompact.size(),
                        inputRows,
                        inputBytes,
                        inputRowGroups,
                        result.size(),
                        outputRows,
                        outputBytes,
                        prepareMs,
                        copyMs,
                        buildResultMs,
                        elapsedMillis(startNanos),
                        options.appendCompactionRowGroupCopyPreservePageIndex());
                return result;
            } catch (IOException | RuntimeException e) {
                cleanupCopiedFiles(fileIO, copiedFiles);
                throw e;
            }
        } catch (IOException e) {
            reportMiss(
                    metrics,
                    MissReason.IO_ERROR,
                    String.format(
                            "inputFiles=%d, inputRows=%d, inputBytes=%d, error=%s",
                            toCompact.size(), inputRows, inputBytes, e.toString()));
            LOG.info("Append compaction fast path failed with IO error, fallback to rewrite", e);
            return null;
        } catch (RuntimeException e) {
            reportMiss(
                    metrics,
                    MissReason.IO_ERROR,
                    String.format(
                            "inputFiles=%d, inputRows=%d, inputBytes=%d, error=%s",
                            toCompact.size(), inputRows, inputBytes, e.toString()));
            LOG.info("Append compaction fast path failed, fallback to rewrite", e);
            return null;
        }
    }

    private static List<DataFileMeta> buildResult(
            List<ParquetRowGroupCopier.OutputFile> copiedFiles,
            List<PreparedInput> preparedInputs,
            List<DataFileMeta> toCompact,
            RowType writeType,
            long schemaId,
            @Nullable List<String> valueStatsCols,
            CoreOptions options,
            DataFilePathFactory pathFactory)
            throws IOException {
        boolean isExternalPath = pathFactory.isExternalPath();
        LongCounter sequenceCounter = new LongCounter(toCompact.get(0).minSequenceNumber());
        List<DataFileMeta> result = new ArrayList<>(copiedFiles.size());
        for (ParquetRowGroupCopier.OutputFile copiedFile : copiedFiles) {
            SimpleStats valueStats =
                    mergeOutputValueStats(
                            copiedFile.blockContributions(),
                            preparedInputs,
                            writeType,
                            valueStatsCols,
                            options);

            long minSequenceNumber = sequenceCounter.getValue();
            sequenceCounter.add(copiedFile.rowCount());
            long maxSequenceNumber = sequenceCounter.getValue() - 1;

            String externalPath = isExternalPath ? copiedFile.path().toString() : null;
            result.add(
                    DataFileMeta.forAppend(
                            copiedFile.path().getName(),
                            copiedFile.fileSize(),
                            copiedFile.rowCount(),
                            valueStats,
                            minSequenceNumber,
                            maxSequenceNumber,
                            schemaId,
                            Collections.emptyList(),
                            null,
                            FileSource.COMPACT,
                            valueStatsCols,
                            externalPath,
                            null,
                            null));
        }

        long inputRowCount = toCompact.stream().mapToLong(DataFileMeta::rowCount).sum();
        long outputRowCount = result.stream().mapToLong(DataFileMeta::rowCount).sum();
        if (inputRowCount != outputRowCount) {
            throw new IOException(
                    String.format(
                            "Row count mismatch after RowGroup copy: input %d but output %d",
                            inputRowCount, outputRowCount));
        }
        return result;
    }

    private static SimpleStats mergeOutputValueStats(
            List<ParquetRowGroupCopier.BlockContribution> contributions,
            List<PreparedInput> preparedInputs,
            RowType writeType,
            @Nullable List<String> valueStatsCols,
            CoreOptions options)
            throws IOException {
        List<SimpleStats> statsToMerge = new ArrayList<>();
        List<BlockMetaData> partialBlocks = new ArrayList<>();
        Map<Integer, List<Integer>> blockIndicesByFile = new LinkedHashMap<>();
        for (ParquetRowGroupCopier.BlockContribution contribution : contributions) {
            blockIndicesByFile
                    .computeIfAbsent(contribution.fileIndex(), ignored -> new ArrayList<>())
                    .add(contribution.blockIndex());
        }

        for (Map.Entry<Integer, List<Integer>> entry : blockIndicesByFile.entrySet()) {
            int fileIndex = entry.getKey();
            PreparedInput preparedInput = preparedInputs.get(fileIndex);
            List<Integer> blockIndices = entry.getValue();
            if (isFullFile(blockIndices, preparedInput.blocks.size())) {
                statsToMerge.add(preparedInput.file.valueStats());
            } else {
                for (int blockIndex : blockIndices) {
                    partialBlocks.add(preparedInput.blocks.get(blockIndex));
                }
            }
        }

        if (!partialBlocks.isEmpty()) {
            statsToMerge.add(statsFromBlocks(partialBlocks, writeType, valueStatsCols, options));
        }
        return SimpleStatsMerger.merge(statsToMerge, writeType, valueStatsCols);
    }

    private static boolean isFullFile(List<Integer> blockIndices, int totalBlocks) {
        if (blockIndices.size() != totalBlocks) {
            return false;
        }
        for (int i = 0; i < totalBlocks; i++) {
            if (blockIndices.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    private static SimpleStats statsFromBlocks(
            List<BlockMetaData> blocks,
            RowType writeType,
            @Nullable List<String> valueStatsCols,
            CoreOptions options)
            throws IOException {
        SimpleColStatsCollector.Factory[] collectors =
                createStatsFactories(options.statsMode(), options, writeType.getFieldNames());
        ParquetSimpleStatsExtractor extractor =
                new ParquetSimpleStatsExtractor(options.toConfiguration(), writeType, collectors);
        SimpleColStats[] colStats = extractor.extractFromBlocks(blocks);
        Pair<List<String>, SimpleStats> converted =
                new SimpleStatsConverter(writeType, options.statsDenseStore()).toBinary(colStats);
        if (!SimpleStatsMerger.sameValueStatsCols(valueStatsCols, converted.getLeft())) {
            throw new IOException(
                    String.format(
                            "Partial block stats columns mismatch: expected %s but got %s",
                            valueStatsCols, converted.getLeft()));
        }
        return converted.getRight();
    }

    private static void cleanupCopiedFiles(
            FileIO fileIO, List<ParquetRowGroupCopier.OutputFile> copiedFiles) {
        for (ParquetRowGroupCopier.OutputFile copiedFile : copiedFiles) {
            fileIO.deleteQuietly(copiedFile.path());
        }
    }

    @Nullable
    private static MissReason checkTableLevel(
            CoreOptions options,
            FileFormat fileFormat,
            int bucket,
            @Nullable List<String> expectedValueStatsCols,
            List<DataFileMeta> toCompact) {
        if (bucket != UNAWARE_BUCKET) {
            return MissReason.OTHER;
        }
        if (!FILE_FORMAT_PARQUET.equals(options.fileFormatString())) {
            return MissReason.OTHER;
        }
        if (!FILE_FORMAT_PARQUET.equals(fileFormat.getFormatIdentifier())) {
            return MissReason.OTHER;
        }
        if (options.rowTrackingEnabled() || options.dataEvolutionEnabled()) {
            return MissReason.ROW_TRACKING;
        }
        if (isBloomFilterConfigured(options)) {
            return MissReason.BLOOM_CONFIGURED;
        }
        if (!options.indexColumnsOptions().isEmpty()) {
            return MissReason.FILE_INDEX;
        }
        if (isParquetWriterV2Configured(options)) {
            return MissReason.OTHER;
        }
        for (DataFileMeta file : toCompact) {
            if (!SimpleStatsMerger.sameValueStatsCols(
                    expectedValueStatsCols, file.valueStatsCols())) {
                return MissReason.OTHER;
            }
        }
        return null;
    }

    @Nullable
    private static MissReason checkFileLevel(
            DataFileMeta file, long schemaId, @Nullable List<String> expectedValueStatsCols) {
        if (file.schemaId() != schemaId) {
            return MissReason.SCHEMA_ID;
        }
        if (!file.extraFiles().isEmpty()) {
            return MissReason.EXTRA_FILES;
        }
        if (file.embeddedIndex() != null) {
            return MissReason.EXTRA_FILES;
        }
        if (file.firstRowId() != null) {
            return MissReason.ROW_TRACKING;
        }
        if (file.writeCols() != null) {
            return MissReason.WRITE_COLS;
        }
        if (file.fileSource().isPresent()) {
            FileSource fileSource = file.fileSource().get();
            if (fileSource != FileSource.APPEND && fileSource != FileSource.COMPACT) {
                return MissReason.FILE_SOURCE;
            }
        }
        if (!SimpleStatsMerger.sameValueStatsCols(expectedValueStatsCols, file.valueStatsCols())) {
            return MissReason.OTHER;
        }
        return null;
    }

    @Nullable
    private static MissReason checkFooterLevel(
            ParquetMetadata footer,
            MessageType expectedSchema,
            String expectedCodec,
            @Nullable CompressionCodecName referenceCodec) {
        FileMetaData fileMetaData = footer.getFileMetaData();
        if (fileMetaData.getEncryptionType() != FileMetaData.EncryptionType.UNENCRYPTED) {
            return MissReason.ENCRYPTION;
        }
        if (hasParquetV2Encoding(footer)) {
            return MissReason.OTHER;
        }
        MessageType fileSchema = fileMetaData.getSchema();
        if (!fileSchema.equals(expectedSchema)) {
            return MissReason.MESSAGE_TYPE;
        }
        CompressionCodecName codec = null;
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData column : block.getColumns()) {
                CompressionCodecName columnCodec = column.getCodec();
                if (codec == null) {
                    codec = columnCodec;
                } else if (!codec.equals(columnCodec)) {
                    return MissReason.CODEC;
                }
                if (!codecMatches(columnCodec, expectedCodec)) {
                    return MissReason.CODEC;
                }
            }
        }
        if (codec == null) {
            codec = CompressionCodecName.UNCOMPRESSED;
        }
        if (referenceCodec != null && !referenceCodec.equals(codec)) {
            return MissReason.CODEC;
        }
        return null;
    }

    private static boolean hasParquetV2Encoding(ParquetMetadata footer) {
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData column : block.getColumns()) {
                if (column.getEncodings().contains(Encoding.BYTE_STREAM_SPLIT)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isParquetWriterV2Configured(CoreOptions options) {
        String writerVersion = options.toConfiguration().get(ParquetOutputFormat.WRITER_VERSION);
        if (writerVersion == null || writerVersion.isEmpty()) {
            return false;
        }
        try {
            return ParquetProperties.WriterVersion.fromString(writerVersion)
                    == ParquetProperties.WriterVersion.PARQUET_2_0;
        } catch (IllegalArgumentException ignored) {
            return "v2".equalsIgnoreCase(writerVersion)
                    || "PARQUET_2_0".equalsIgnoreCase(writerVersion);
        }
    }

    private static boolean codecMatches(CompressionCodecName codec, String expectedCodec) {
        if ("none".equalsIgnoreCase(expectedCodec)
                || "uncompressed".equalsIgnoreCase(expectedCodec)) {
            return codec == CompressionCodecName.UNCOMPRESSED;
        }
        return codec.name().equalsIgnoreCase(expectedCodec)
                || codec.toString().equalsIgnoreCase(expectedCodec);
    }

    private static boolean isBloomFilterConfigured(CoreOptions options) {
        Map<String, String> config = options.toConfiguration().toMap();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            if (ParquetOutputFormat.BLOOM_FILTER_ENABLED.equals(key)
                    && Boolean.parseBoolean(entry.getValue())) {
                return true;
            }
            if (key.startsWith(ParquetOutputFormat.BLOOM_FILTER_ENABLED + "#")
                    && Boolean.parseBoolean(entry.getValue())) {
                return true;
            }
            if ("parquet.bloom.filter.columns".equals(key) && !entry.getValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static String normalizeCompression(String compression) {
        if (compression == null || compression.isEmpty()) {
            return CompressionCodecName.UNCOMPRESSED.name().toLowerCase();
        }
        if ("none".equalsIgnoreCase(compression) || "uncompressed".equalsIgnoreCase(compression)) {
            return CompressionCodecName.UNCOMPRESSED.name().toLowerCase();
        }
        return compression.toLowerCase();
    }

    private static void reportMiss(
            @Nullable CompactionFastPathMetrics metrics, MissReason reason, String detail) {
        if (metrics != null) {
            metrics.reportMiss(reason);
        }
        LOG.info("Append compaction fast path miss: reason={}, {}", reason, detail);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static final class PreparedInput {
        private final DataFileMeta file;
        private final ParquetInputFile inputFile;
        private final ParquetMetadata metadata;
        private final List<BlockMetaData> blocks;
        private final CompressionCodecName codec;

        private PreparedInput(
                DataFileMeta file,
                ParquetInputFile inputFile,
                ParquetMetadata metadata,
                CompressionCodecName codec) {
            this.file = file;
            this.inputFile = inputFile;
            this.metadata = metadata;
            this.blocks = metadata.getBlocks();
            this.codec = codec;
        }
    }
}
