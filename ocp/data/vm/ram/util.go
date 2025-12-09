package ram

import (
	"math"

	"github.com/code-payments/ocp-server/solana/vm"
)

func GetActualCapcity(record *Record) uint16 {
	sizeInMemory := int(vm.GetVirtualAccountSizeInMemory(record.StoredAccountType))
	pagesPerAccount := math.Ceil(1 / (float64(record.PageSize) / float64(sizeInMemory)))
	availablePerSector := int(record.NumPages) / int(pagesPerAccount)
	maxAvailableAcrossSectors := uint16(record.NumSectors) * uint16(availablePerSector)
	if record.Capacity < maxAvailableAcrossSectors {
		return record.Capacity
	}
	return maxAvailableAcrossSectors
}
