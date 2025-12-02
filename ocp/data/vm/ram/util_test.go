package ram

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/code-payments/ocp-server/solana/vm"
)

func TestGetActualCapcity(t *testing.T) {
	for _, tc := range []struct {
		capacity   uint16
		numSectors uint16
		numPages   uint16
		pageSize   uint8
		expected   uint16
	}{
		{
			capacity:   1000,
			numSectors: 2,
			numPages:   50,
			pageSize:   uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
			expected:   100,
		},
		{
			capacity:   10,
			numSectors: 2,
			numPages:   50,
			pageSize:   uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)),
			expected:   10,
		},
		{
			capacity:   1000,
			numSectors: 2,
			numPages:   50,
			pageSize:   uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)) - 1,
			expected:   50,
		},
		{
			capacity:   1000,
			numSectors: 2,
			numPages:   50,
			pageSize:   uint8(vm.GetVirtualAccountSizeInMemory(vm.VirtualAccountTypeTimelock)) / 3,
			expected:   24,
		},
	} {
		record := &Record{
			Capacity:          tc.capacity,
			NumSectors:        tc.numSectors,
			NumPages:          tc.numPages,
			PageSize:          tc.pageSize,
			StoredAccountType: vm.VirtualAccountTypeTimelock,
		}
		actual := GetActualCapcity(record)
		assert.Equal(t, tc.expected, actual)
	}
}
