package meta

type FileMeta struct {
	FileSha1     string `field:"文件唯一值"`
	FileName     string `filed:"文件名称"`
	FileSize     int64  `field:"文件大小"` //bit
	Tmp_Location string `field:"文件临时路径"`
	UpdatedAt    string
	ChunkNum     int
}

var fileMetas map[string]FileMeta

func init() {
	fileMetas = make(map[string]FileMeta)
}

// UpdateFileMeta:新增/更新文件元信息
func UpdateFileMeta(fmeta FileMeta) {
	fileMetas[fmeta.FileSha1] = fmeta
}

// GetFileMeta:通过sha1值获取文件的元信息对象
func GetFileMeta(fileSha1 string) FileMeta {
	return fileMetas[fileSha1]
}
