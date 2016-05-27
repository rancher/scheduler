package cache

type DiskReserved struct {
	DevicePath        string
	ReadIopsReserved  uint64
	WriteIopsReserved uint64
}

type InstanceInfo struct {
	InstanceId      string
	CpuReserved     float64
	MemReservedInMB float64

	// key: device name (/dev/sda) without /dev, so just sda
	// value: reserved iops for read
	DisksReservedMap map[string]*DiskReserved
}

const (
	ReadIopsLabel  = "io.rancher.resource.read_iops"
	WriteIopsLabel = "io.rancher.resource.write_iops"
)
