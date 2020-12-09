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

-- data KVStore k v
--   = MemoryKVStore (InMemoryKVStore k v)
--   | OtherStore
--
-- data DKVStore
--   = DMemoryKVStore DInMemoryKVStore
--   | DOtherStore
--
-- mkDKVStore ::
--   (Typeable k, Typeable v, Ord k) => KVStore k v -> DKVStore
-- mkDKVStore kvStore =
--   case kvStore of
--     MemoryKVStore memoryStore -> DMemoryKVStore $ mkDInMemoryKVStore memoryStore
--     OtherStore -> throw $ UnSupportedStateStoreError "unsupported state store"
--
-- fromDKVStore ::
--   (Typeable k, Typeable v, Ord k) => DKVStore -> KVStore k v
-- fromDKVStore dkvStore =
--   case dkvStore of
--     DMemoryKVStore (DInMemoryKVStore store) ->
--       case cast store of
--         Just s -> s
--         Nothing -> throw $ TypeCastError "type cast error"
--     DOtherStore -> OtherStore
--
-- data InMemoryKVStore k v = InMemoryKVStore
--   { imksData :: IORef (Map.Map k v)
--   }
--
-- mkInMemoryKVStore :: IO (InMemoryKVStore k v)
-- mkInMemoryKVStore = do
--   internalData <- newIORef Map.empty
--   return
--     InMemoryKVStore
--       { imksData = internalData
--       }
--
-- data KVStoreI s k v = KVStoreI
--   { ksiGet :: k -> s k v -> IO (Maybe v),
--     ksiPut :: k -> v -> s k v -> IO ()
--   }
--
-- inMemoryKVStoreI ::
--   (Typeable k, Typeable v, Ord k) =>
--   KVStoreI InMemoryKVStore k v
-- inMemoryKVStoreI =
--   KVStoreI
--     { ksiGet =
--         ( \k InMemoryKVStore {..} -> do
--             dict <- readIORef imksData
--             return $ Map.lookup k dict
--         ),
--       ksiPut =
--         ( \k v InMemoryKVStore {..} -> do
--             dict <- readIORef imksData
--             writeIORef imksData (Map.insert k v dict)
--         )
--     }
--
-- data DInMemoryKVStore
--   = forall k v.
--     (Typeable k, Typeable v, Ord k) =>
--     DInMemoryKVStore (InMemoryKVStore k v)
--
-- mkDInMemoryKVStore ::
--   (Typeable k, Typeable v, Ord k) =>
--   InMemoryKVStore k v ->
--   DInMemoryKVStore
-- mkDInMemoryKVStore store = DInMemoryKVStore store
--
-- -- data DInMemoryKVStore where
-- --   DInMemoryKVStore ::
-- --     forall k v.
-- --     (Typeable k, Typeable v) =>
-- --     InMemoryKVStore k v ->
-- --     DInMemoryKVStore
--
-- dksGet ::
--   (Typeable k, Typeable v, Ord k) =>
--   k ->
--   DInMemoryKVStore ->
--   IO (Maybe v)
-- dksGet k (DInMemoryKVStore store) =
--   case cast store of
--     Just s -> ksiGet inMemoryKVStoreI k s
--     Nothing -> error "dksGet cast error"
--
-- dksPut ::
--   (Typeable k, Typeable v, Ord k) =>
--   k ->
--   v ->
--   DInMemoryKVStore ->
--   IO ()
-- dksPut k v (DInMemoryKVStore store) =
--   case cast store of
--     Just s -> ksiPut inMemoryKVStoreI k v s
--     Nothing -> error "cast error"

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

-- 这种转换的思路不对.
-- 应该把 DEKVStore 实现成一个 KVStore 的 instance.
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

-- def :: DEStore -> IO ()
-- def (DEStore estore) =
--   case cast estore of
--     Just (EStore s) -> do
--       put "hello" True s
--       mr <- get "hello" s
--       forM_ mr printBool
--     Nothing -> error "cast error"
