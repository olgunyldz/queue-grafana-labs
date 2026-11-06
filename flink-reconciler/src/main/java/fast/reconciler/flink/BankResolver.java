package fast.reconciler.flink;

import java.util.Map;

/** Keeps the bank-code and IBAN fallback semantics of the Kafka Streams reconciler. */
final class BankResolver {
    static final String UNKNOWN = "other";

    private static final Map<String, String> NAMES = Map.ofEntries(
            Map.entry("00010", "Ziraat Bankasi"),
            Map.entry("00012", "Halkbank"),
            Map.entry("00015", "Vakifbank"),
            Map.entry("00032", "TEB"),
            Map.entry("00046", "Akbank"),
            Map.entry("00062", "Garanti BBVA"),
            Map.entry("00064", "Is Bankasi"),
            Map.entry("00067", "Yapi Kredi"),
            Map.entry("00099", "ING Bank"),
            Map.entry("00111", "QNB / Enpara.com"),
            Map.entry("00134", "Denizbank"),
            Map.entry("00203", "Albaraka Turk"),
            Map.entry("00205", "Kuveyt Turk"),
            Map.entry("00206", "Turkiye Finans"),
            Map.entry("00209", "Ziraat Katilim"),
            Map.entry("00210", "Vakif Katilim"),
            Map.entry("00231", "Papara"),
            Map.entry("00238", "Hayhay"));

    private BankResolver() {
    }

    static String code(String explicitCode, String iban) {
        if (explicitCode != null && !explicitCode.isBlank()) {
            return explicitCode;
        }
        if (iban != null && iban.length() >= 9 && iban.startsWith("TR")) {
            return iban.substring(4, 9);
        }
        return null;
    }

    static String[] labels(String code) {
        if (code != null && NAMES.containsKey(code)) {
            return new String[] {code, NAMES.get(code)};
        }
        return new String[] {UNKNOWN, UNKNOWN};
    }
}
