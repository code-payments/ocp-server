package vm

func GetVirtualAccountSize(accountType VirtualAccountType) uint32 {
	switch accountType {
	case VirtualAccountTypeDurableNonce:
		return VirtualDurableNonceSize
	case VirtualAccountTypeTimelock:
		return VirtualTimelockAccountSize
	default:
		return 0
	}
}

func GetVirtualAccountSizeInMemory(accountType VirtualAccountType) uint32 {
	return GetVirtualAccountSize(accountType) + 1
}
