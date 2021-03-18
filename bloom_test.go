package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"testing"
)

var (
	BLOOM_TSET = "bloom_test"
	TOKEN_PRE  = "token_%v"
	TOKEN_PRE2 = "token_2_%v"

	KEY_NUMBER = 100000

	server_addr = "127.0.0.1:6380"
)

//func TestBloom(t *testing.T) {
//	conn, err := redis.Dial("tcp", server_addr)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	t.Logf("Redis client dial success, %v", server_addr)
//
//	reply, err := conn.Do("new", BLOOM_TSET, KEY_NUMBER, 0.0000001)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	t.Log(redis.String(reply, err))
//
//	for i := 0; i < KEY_NUMBER; i++ {
//		reply, err = conn.Do("add", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE, i))
//		if err != nil {
//			t.Fatal(err.Error())
//		}
//		//t.Log(redis.String(reply, err))
//	}
//
//	finded := 0
//	for i := 0; i < KEY_NUMBER; i++ {
//		reply, err = conn.Do("test", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE, i))
//		if err != nil {
//			t.Fatal(err.Error())
//		}
//
//		val, err := redis.Int64(reply, err)
//		if err != nil {
//			t.Fatal(err.Error())
//		}
//
//		if val == int64(1) {
//			finded += 1
//			//t.Fatal("unexpected value")
//		}
//	}
//
//	t.Log(finded)
//	if float64(KEY_NUMBER-finded)/float64(KEY_NUMBER) > 0.00000001 {
//		t.Fatal("unexpected value")
//	}
//
//	reply, err = conn.Do("del", BLOOM_TSET)
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//	t.Log(redis.Int64(reply, err))
//
//	err = conn.Close()
//	if err != nil {
//		t.Fatal(err.Error())
//	}
//
//	//for i := 0; i < KEY_NUMBER; i++ {
//	//	reply, err = conn.Do("test", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE2, i))
//	//	if err != nil {
//	//		t.Fatal(err.Error())
//	//	}
//	//
//	//	val, err := redis.Int64(reply, err)
//	//	if err != nil {
//	//		t.Fatal(err.Error())
//	//	}
//	//
//	//	if val != int64(0) {
//	//		t.Fatal("unexpected value")
//	//	}
//	//}
//}

func BenchmarkBloom(b *testing.B) {
	conn, err := redis.Dial("tcp", server_addr)
	if err != nil {
		b.Fatal(err.Error())
	}
	//b.Logf("Redis client dial success, %v", server_addr)

	reply, err := conn.Do("new", BLOOM_TSET, KEY_NUMBER, 0.0000001)
	if err != nil {
		b.Fatal(err.Error())
	}
	b.Log(redis.String(reply, err))

	for i := 0; i < KEY_NUMBER; i++ {
		reply, err = conn.Do("add", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE, i))
		if err != nil {
			b.Fatal(err.Error())
		}
		//b.Log(redis.String(reply, err))
	}
	b.Log("keys created")

	conn.Close()

	b.ResetTimer()
	b.Run("client_add", func(b *testing.B) {
		benchBloomAdd(b, b.N)
	})

	b.ResetTimer()
	b.Run("client_test", func(b *testing.B) {
		benchBloomOneTest(b, b.N)
	})
	b.Log("SUCCESS")
}

func benchBloomAdd(t *testing.B, keysNumber int) {
	conn, err := redis.Dial("tcp", server_addr)
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 0; i < keysNumber; i++ {
		reply, err := conn.Do("add", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE, i))
		if err != nil {
			t.Fatal(err.Error())
		}

		result, err := redis.String(reply, err)
		if err != nil {
			t.Fatal(err.Error())
		}

		if result != "OK" {
			t.Fatalf(result)
		}
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func benchBloomOneTest(t *testing.B, keysNumber int) {
	conn, err := redis.Dial("tcp", server_addr)
	if err != nil {
		t.Fatal(err.Error())
	}
	//t.Logf("Redis client dial success, %v", server_addr)

	finded := 0
	for i := 0; i < keysNumber; i++ {
		reply, err := conn.Do("test", BLOOM_TSET, fmt.Sprintf(TOKEN_PRE, i))
		if err != nil {
			t.Fatal(err.Error())
		}

		val, err := redis.Int64(reply, err)
		if err != nil {
			t.Fatal(err.Error())
		}

		if val == int64(1) {
			finded += 1
			//t.Fatal("unexpected value")
		}
	}

	//t.Log(finded)

	err = conn.Close()
	if err != nil {
		t.Fatal(err.Error())
	}
}
