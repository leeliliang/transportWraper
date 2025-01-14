package transport

import "github.com/google/uuid"

func GenerateUUID() string {
	// 生成一个新的 UUID
	uuidInstance := uuid.New()
	// 将 UUID 转换为字符串
	return uuidInstance.String()
}
