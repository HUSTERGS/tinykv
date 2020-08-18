package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	conf * config.Config
}

type StandAloneStorageReader struct {
	// FIXME: 暂时先不管raft
	kvTxn  * badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 由于stop方法需要保证两个都有，所以需要两个都初始化
	s.engine = engine_util.NewEngines(engine_util.CreateDB("kv", s.conf),
		engine_util.CreateDB("raft", s.conf), "kv", "raft")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// 不知道比不必要，关于Log的使用
	if err := s.engine.Close(); err != nil {
		//log.Fatal(err)
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 暂时不管ctx
	return &StandAloneStorageReader{
		kvTxn: s.engine.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 暂时不用管这个ctx
	txn := s.engine.Kv.NewTransaction(true)
	for _, m := range batch {
		// 此处参照modify.go
		switch m.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value()); err != nil{
				return err
			}
		case storage.Delete:
			if err :=txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key())); err != nil{
				return err
			}
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (reader * StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.kvTxn, cf, key)
	if err != nil {
		// 这个地方需要手动写一个判断，由于测试`TestRawDelete1`中要求没有找到的时候返回nil, nil
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	return val, nil
}


func (reader * StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.kvTxn)
}

func (reader * StandAloneStorageReader) Close () {
	reader.kvTxn.Discard()
}