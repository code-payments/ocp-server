package balance

type AccountBalanceRecord struct {
	Quarks       uint64
	UsdCostBasis float64
	Version      uint64
}

func (r *AccountBalanceRecord) Clone() AccountBalanceRecord {
	return AccountBalanceRecord{
		Quarks:       r.Quarks,
		UsdCostBasis: r.UsdCostBasis,
		Version:      r.Version,
	}
}
