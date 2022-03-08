package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/google/uuid"
	connection "github.com/serqol/go-pool"
	"github.com/serqol/go-utils"
)

var connector *connection.Connector
var once sync.Once

func init() {
	utils.LoadEnv()
}

func Get(name string) string {
	result, error := GetConnector().Execute(func(connection interface{}) (interface{}, error) {
		result, error := connection.(*redis.Client).Get(name).Result()
		return result, processError(error)
	})

	if error != nil {
		fmt.Print(error)
		return ""
	}

	return result.(string)
}

func Set(name string, value string, ttl time.Duration) {
	_, error := GetConnector().Execute(func(connection interface{}) (interface{}, error) {
		return connection.(*redis.Client).Set(name, value, ttl).Result()
	})

	if error != nil {
		fmt.Print(error)
	}
}

func GenerateUniqueKey(value interface{}, ttl time.Duration, prefix string) (string, error) {
	for tries := 100; tries > 0; tries-- {
		uuid := uuid.New().String()
		result, error := GetConnector().Execute(func(connection interface{}) (interface{}, error) {
			return connection.(*redis.Client).SetNX(prefix+uuid, value, ttl).Result()
		})
		if error != nil {
			break
		}
		if result.(bool) {
			return uuid, nil
		}
	}
	return "", errors.New("failed to generate unique uuid")
}

func StoreObject(name string, ttl time.Duration, object interface{}) {
	objectBytes, error := json.Marshal(object)

	if error != nil {
		fmt.Print(error)
		return
	}

	Set(name, string(objectBytes), ttl)
}

func GetObject(name string, object interface{}) interface{} {
	raw := Get(name)
	if raw == "" {
		return nil
	}
	error := json.Unmarshal([]byte(raw), &object)

	if error != nil {
		fmt.Print(error)
		return nil
	}

	return object
}

func Delete(key string) {
	_, error := GetConnector().Execute(func(connection interface{}) (interface{}, error) {
		return connection.(*redis.Client).Del(key).Result()
	})

	if error != nil {
		fmt.Print(error)
	}
}

func processError(errorObj error) error {
	switch errorObj.(type) {
	case *net.OpError:
		return errorObj
	}
	return nil
}

func connectorFunction() func() (interface{}, error) {
	return func() (interface{}, error) {
		host, _ := utils.GetEnv("REDIS_HOST", "redis").(string)
		port, _ := utils.GetEnv("REDIS_PORT", "6379").(string)
		pass, _ := utils.GetEnv("REDIS_PASSWORD", "").(string)
		addr := fmt.Sprintf("%s:%s", host, port)

		return redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: pass,
			DB:       0,
		}), nil
	}
}

func GetConnector() *connection.Connector {
	once.Do(func() {
		connector = connection.GetConnector(connectorFunction(), nil, 4, 200, 3600)
	})
	return connector
}
