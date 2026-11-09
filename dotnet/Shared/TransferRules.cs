namespace Shared;

/// <summary>
/// Decides whether a transfer event is accepted. Havale is a transfer between accounts of the same bank;
/// EFT is a transfer between two different banks. FAST has no bank rule.
/// </summary>
public static class TransferRules
{
    public static string? Validate(string eventName, Transfer t)
    {
        if (string.IsNullOrWhiteSpace(t.TransactionId)) return "transaction_id is required";
        if (t.Amount <= 0) return "amount must be greater than 0";
        if (string.IsNullOrEmpty(t.Currency)) t.Currency = "TRY";
        if (t.Currency != "TRY") return "transfer events must be in TRY";
        if (eventName.EndsWith("_failed", StringComparison.Ordinal) && string.IsNullOrWhiteSpace(t.FailureReason))
            return "failure_reason is required for failed events";

        bool havale = eventName.StartsWith("havale_", StringComparison.Ordinal);
        bool eft = eventName.StartsWith("eft_", StringComparison.Ordinal);
        if (!havale && !eft) return null;

        string? sender = BankCode(t.SenderBank, t.SenderIban);
        string? receiver = BankCode(t.ReceiverBank, t.ReceiverIban);
        if (sender is null || receiver is null)
            return "sender and receiver bank (or IBAN) are required for havale and eft";
        if (havale && sender != receiver)
            return "havale is a transfer between accounts of the same bank: sender and receiver bank must be equal";
        if (eft && sender == receiver)
            return "eft is a transfer between two banks: sender and receiver bank must differ";
        return null;
    }

    /// <summary>
    /// The bank code: the bank field when given, otherwise IBAN positions 5-9 (TR, two check digits, then the
    /// five-digit bank code). The same rule as the Flink reconciler's BankResolver.
    /// </summary>
    public static string? BankCode(string? bank, string? iban)
    {
        if (!string.IsNullOrWhiteSpace(bank)) return bank.Trim();
        string value = iban?.Trim().Replace(" ", "") ?? "";
        return value.Length >= 9 && value.StartsWith("TR", StringComparison.OrdinalIgnoreCase)
            ? value.Substring(4, 5)
            : null;
    }
}
