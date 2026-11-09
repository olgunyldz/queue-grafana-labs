using Shared;
using Xunit;

namespace Shared.Tests;

public class TransferRulesTests
{
    private static Transfer Transfer(string? senderBank, string? receiverBank, string? senderIban = null,
        string? receiverIban = null, string? failureReason = null) => new()
    {
        TransactionId = "tx-1",
        Amount = 100m,
        Currency = "TRY",
        SenderBank = senderBank,
        ReceiverBank = receiverBank,
        SenderIban = senderIban,
        ReceiverIban = receiverIban,
        FailureReason = failureReason,
    };

    [Fact]
    public void HavaleBetweenAccountsOfTheSameBankIsAccepted()
    {
        Assert.Null(TransferRules.Validate("havale_request", Transfer("00010", "00010")));
        Assert.Null(TransferRules.Validate("havale_success", Transfer("00010", "00010")));
    }

    [Fact]
    public void HavaleBetweenDifferentBanksIsRejected()
    {
        var error = TransferRules.Validate("havale_request", Transfer("00010", "00046"));
        Assert.NotNull(error);
        Assert.Contains("same bank", error);
    }

    [Fact]
    public void EftBetweenTwoBanksIsAccepted()
    {
        Assert.Null(TransferRules.Validate("eft_incoming_request", Transfer("00010", "00046")));
        Assert.Null(TransferRules.Validate("eft_outgoing_success", Transfer("00010", "00046")));
    }

    [Fact]
    public void EftWithinOneBankIsRejected()
    {
        var error = TransferRules.Validate("eft_outgoing_request", Transfer("00010", "00010"));
        Assert.NotNull(error);
        Assert.Contains("must differ", error);
    }

    [Fact]
    public void BankIsTakenFromTheIbanWhenTheBankFieldIsMissing()
    {
        // TR33 0006 1 ... : bank code 00061 (positions 5-9).
        const string iban = "TR330006100519786457841326";
        Assert.Equal("00061", TransferRules.BankCode(null, iban));
        Assert.Null(TransferRules.Validate("havale_request", Transfer(null, "00061", senderIban: iban)));
        Assert.NotNull(TransferRules.Validate("havale_request", Transfer(null, "00046", senderIban: iban)));
    }

    [Fact]
    public void HavaleAndEftNeedABankOrAnIban()
    {
        var error = TransferRules.Validate("havale_request", Transfer(null, "00010"));
        Assert.NotNull(error);
        Assert.Contains("required", error);
    }

    [Fact]
    public void FastHasNoBankRule()
    {
        Assert.Null(TransferRules.Validate("fast_incoming_request", Transfer("00010", "00046")));
        Assert.Null(TransferRules.Validate("fast_outgoing_request", Transfer("00010", "00010")));
    }

    [Fact]
    public void FailedEventsStillNeedAFailureReason()
    {
        Assert.NotNull(TransferRules.Validate("havale_failed", Transfer("00010", "00010")));
        Assert.Null(TransferRules.Validate("havale_failed", Transfer("00010", "00010", failureReason: "INSUFFICIENT_BALANCE")));
    }
}
