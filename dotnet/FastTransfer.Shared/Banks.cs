namespace FastTransfer.Shared;

/// <summary>
/// Bank codes (IBAN positions 5-9) mapped to display names. Mirrors internal/fast/banks.go.
/// Enpara.com runs on QNB's infrastructure and shares code 00111, so the two are reported together.
/// </summary>
public static class Banks
{
    public const string Unknown = "other";

    public static readonly IReadOnlyDictionary<string, string> Names = new Dictionary<string, string>
    {
        ["00010"] = "Ziraat Bankası",
        ["00012"] = "Halkbank",
        ["00015"] = "Vakıfbank",
        ["00032"] = "TEB",
        ["00046"] = "Akbank",
        ["00062"] = "Garanti BBVA",
        ["00064"] = "İş Bankası",
        ["00067"] = "Yapı Kredi",
        ["00099"] = "ING Bank",
        ["00111"] = "QNB / Enpara.com",
        ["00134"] = "Denizbank",
        ["00203"] = "Albaraka Türk",
        ["00205"] = "Kuveyt Türk",
        ["00206"] = "Türkiye Finans",
        ["00209"] = "Ziraat Katılım",
        ["00210"] = "Vakıf Katılım",
        ["00231"] = "Papara",
        ["00238"] = "Hayhay",
    };

    /// <summary>Bank code from the explicit code or, if empty, from a TR IBAN (TRkk BBBBB ...).</summary>
    public static string? Code(string? code, string? iban)
    {
        if (!string.IsNullOrEmpty(code)) return code;
        if (iban is { Length: >= 9 } && iban.StartsWith("TR", StringComparison.Ordinal)) return iban.Substring(4, 5);
        return null;
    }

    /// <summary>(code, name) metric labels; unknown codes fold into "other" to bound cardinality.</summary>
    public static (string Code, string Name) Labels(string code) =>
        Names.TryGetValue(code, out var name) ? (code, name) : (Unknown, Unknown);
}
