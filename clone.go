package configrollout

import "encoding/json"

// cloneData 返回快照的深拷贝，使 MemoryStore 与调用方互不留引用。
func cloneData(d *Data) *Data {
	raw, err := json.Marshal(d)
	if err != nil {
		// Data 仅包含可 JSON 序列化的类型；若不可序列化属于编程错误。
		panic("configrollout: clone data: " + err.Error())
	}
	var out Data
	if err := json.Unmarshal(raw, &out); err != nil {
		panic("configrollout: clone data: " + err.Error())
	}
	return &out
}
