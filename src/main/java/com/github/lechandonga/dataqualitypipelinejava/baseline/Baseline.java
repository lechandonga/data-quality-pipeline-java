package com.github.lechandonga.dataqualitypipelinejava.baseline;

/**
 * 历史基线：按字段维度存储的聚合统计特征。
 *
 * <p>基线是逐批次累积合并（而非简单覆盖）的结果：
 * count/nonNull/均值/二阶矩按已观测样本合并，保证可复现。</p>
 *
 * @param field         字段名
 * @param batchCount    已合并的成功批次数
 * @param totalCount    累计记录数
 * @param nonNullCount  累计非空记录数
 * @param numericCount  累计可解析为数值的记录数
 * @param mean          数值均值
 * @param m2            Welford 二阶矩
 * @param distinctCount 基数（截断后仅为下界）
 */
public record Baseline(
        String field,
        long batchCount,
        long totalCount,
        long nonNullCount,
        long numericCount,
        double mean,
        double m2,
        long distinctCount) {
}
