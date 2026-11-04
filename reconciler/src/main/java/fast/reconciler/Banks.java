package fast.reconciler;

import java.util.Map;

/**
 * Bank code (IBAN positions 5-9) to display name; the single bank list of the project.
 * Enpara.com runs on QNB's infrastructure and shares code 00111, so the two are reported together.
 */
final class Banks {
    static final String UNKNOWN = "other";

    private static final Map<String, String> NAMES = Map.ofEntries(
            Map.entry("00010", "Ziraat Bankası"),
            Map.entry("00012", "Halkbank"),
            Map.entry("00015", "Vakıfbank"),
            Map.entry("00032", "TEB"),
            Map.entry("00046", "Akbank"),
            Map.entry("00062", "Garanti BBVA"),
            Map.entry("00064", "İş Bankası"),
            Map.entry("00067", "Yapı Kredi"),
            Map.entry("00099", "ING Bank"),
            Map.entry("00111", "QNB / Enpara.com"),
            Map.entry("00134", "Denizbank"),
            Map.entry("00203", "Albaraka Türk"),
            Map.entry("00205", "Kuveyt Türk"),
            Map.entry("00206", "Türkiye Finans"),
            Map.entry("00209", "Ziraat Katılım"),
            Map.entry("00210", "Vakıf Katılım"),
            Map.entry("00231", "Papara"),
            Map.entry("00238", "Hayhay"));

    private Banks() {
    }

    /** Bank code from the explicit field or, if empty, from a TR IBAN; null when neither is usable. */
    static String code(String code, String iban) {
        if (code != null && !code.isBlank()) {
            return code;
        }
        if (iban != null && iban.length() >= 9 && iban.startsWith("TR")) {
            return iban.substring(4, 9);
        }
        return null;
    }

    /** Metric label pair; unknown codes fold into "other" to keep label cardinality bounded. */
    static String[] labels(String code) {
        if (code != null && NAMES.containsKey(code)) {
            return new String[] {code, NAMES.get(code)};
        }
        return new String[] {UNKNOWN, UNKNOWN};
    }
}
