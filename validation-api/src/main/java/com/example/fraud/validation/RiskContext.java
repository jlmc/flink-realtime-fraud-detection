package com.example.fraud.validation;

import java.math.BigDecimal;
import java.util.List;

/**
 * Domain-level view of a customer's recent behaviour, computed by the stream processor from its
 * managed state. Risk rules never see Flink state.
 *
 * @param transactionsLastMinute   transactions in the 1 minute window ending at the current event (inclusive)
 * @param amountLastTenMinutes     total spent in the 10 minute window ending at the current event (inclusive)
 * @param previousCountry          country of the previous transaction, or {@code null} if none
 * @param recentCountries          countries of recent transactions in chronological order, oldest first,
 *                                 excluding the current one
 * @param averageRecentAmount      mean amount of recent transactions excluding the current one,
 *                                 or {@code null} if there is no history
 * @param recentTransactionCount   number of transactions that make up {@code averageRecentAmount}
 */
public record RiskContext(
        int transactionsLastMinute,
        BigDecimal amountLastTenMinutes,
        String previousCountry,
        List<String> recentCountries,
        BigDecimal averageRecentAmount,
        int recentTransactionCount) {

    public RiskContext {
        recentCountries = List.copyOf(recentCountries);
    }
}
