{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Store
  ( KVStore (..),
    InMemoryKVStore,
    mkInMemoryKVStore,
    mkDEKVStore,
    fromDEKVStore,
    EKVStore,
    DEKVStore,
  )
where

import Control.Exception (throw)
import Data.Typeable
import HStream.Error
import RIO
import qualified RIO.Map as Map

data InMemoryKVStore k v = InMemoryKVStore
  { imksData :: IORef (Map k v)
  }

mkInMemoryKVStore :: IO (InMemoryKVStore k v)
mkInMemoryKVStore = do
  internalData <- newIORef Map.empty
  return
    InMemoryKVStore
      { imksData = internalData
      }

class KVStore s where
  ksGet :: Ord k => k -> s k v -> IO (Maybe v)
  ksPut :: Ord k => k -> v -> s k v -> IO ()

instance KVStore InMemoryKVStore where
  ksGet k InMemoryKVStore {..} = do
    dict <- readIORef imksData
    return $ Map.lookup k dict

  ksPut k v InMemoryKVStore {..} = do
    dict <- readIORef imksData
    writeIORef imksData (Map.insert k v dict)

data EKVStore k v
  = forall s.
    KVStore s =>
    EKVStore (s k v)

data DEKVStore
  = forall k v.
    (Typeable k, Typeable v) =>
    DEKVStore (EKVStore k v)

instance KVStore EKVStore where
  ksGet k (EKVStore s) = ksGet k s

  ksPut k v (EKVStore s) = ksPut k v s

mkDEKVStore ::
  (KVStore s, Typeable k, Typeable v, Ord k) =>
  s k v ->
  DEKVStore
mkDEKVStore store = DEKVStore (EKVStore store)

fromDEKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  DEKVStore ->
  EKVStore k v
fromDEKVStore (DEKVStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing -> throw $ TypeCastError "fromDEKVStore: type cast error"

