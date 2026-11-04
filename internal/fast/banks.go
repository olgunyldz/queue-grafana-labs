// Package fast holds shared definitions for FAST (Fonların Anlık ve Sürekli
// Transferi) transfers.
package fast

// Banks maps the 5 digit bank code (IBAN positions 5-9) to a display name.
// Enpara.com runs on QNB's infrastructure and shares code 00111, so the two
// cannot be told apart from the IBAN and are reported together.
var Banks = map[string]string{
	"00010": "Ziraat Bankası",
	"00012": "Halkbank",
	"00015": "Vakıfbank",
	"00032": "TEB",
	"00046": "Akbank",
	"00062": "Garanti BBVA",
	"00064": "İş Bankası",
	"00067": "Yapı Kredi",
	"00099": "ING Bank",
	"00111": "QNB / Enpara.com",
	"00134": "Denizbank",
	"00203": "Albaraka Türk",
	"00205": "Kuveyt Türk",
	"00206": "Türkiye Finans",
	"00209": "Ziraat Katılım",
	"00210": "Vakıf Katılım",
	"00231": "Papara",
	"00238": "Hayhay",
}

// UnknownBank is used for codes that are not in Banks. Folding unknown codes
// into a single label keeps Prometheus label cardinality bounded.
const UnknownBank = "other"

// BankCode returns the bank code from the transfer's explicit code or, if that
// is empty, from a TR IBAN (TRkk BBBBB ...). It returns "" when neither is usable.
func BankCode(code, iban string) string {
	if code != "" {
		return code
	}
	if len(iban) >= 9 && iban[:2] == "TR" {
		return iban[4:9]
	}
	return ""
}

// BankLabels returns the (code, name) pair used as metric labels.
func BankLabels(code string) (string, string) {
	if name, ok := Banks[code]; ok {
		return code, name
	}
	return UnknownBank, UnknownBank
}
