package kvraft

type KvService struct {
	Data map[string]string
}

func NewKvService() KvService {
	return KvService{
		Data: make(map[string]string),
	}
}

func (kv *KvService) get(key string) (string, Err) {
	if _, ok := kv.Data[key]; !ok {
		return "", ErrNoKey
	}
	return kv.Data[key], OK
}

func (kv *KvService) put(key string, value string) Err {
	kv.Data[key] = value
	return OK
}

func (kv *KvService) append(key string, value string) Err {
	kv.Data[key] += value
	return OK
}
