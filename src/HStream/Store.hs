{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Store
  ( KVStore (..),
    SessionStore (..),
    StateStore,
    InMemoryKVStore,
    -- mkInMemoryKVStore,
    mkInMemoryStateKVStore,
    mkDEKVStore,
    EKVStore,
    DEKVStore,
    InMemorySessionStore,
    -- mkInMemorySessionStore,
    mkInMemoryStateSessionStore,
    fromEStateStoreToKVStore,
    fromEStateStoreToSessionStore,
    wrapStateStore,
    EStateStore,
    ESessionStore,
  )
where

import Control.Exception (throw)
import Data.Maybe
import Data.Typeable
import HStream.Error
import HStream.Stream.SessionWindows
import HStream.Stream.TimeWindows
import HStream.Type
import RIO
import qualified RIO.Map as Map
import qualified RIO.Text as T

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

fromDEKVStoreToEKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  DEKVStore ->
  EKVStore k v
fromDEKVStoreToEKVStore (DEKVStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing -> throw $ TypeCastError "fromDEKVStoreToEKVStore: type cast error"

data StateStore k v
  = KVStateStore (EKVStore k v)
  | SessionStateStore (ESessionStore k v)

data EStateStore
  = EKVStateStore DEKVStore
  | ESessionStateStore DESessionStore

wrapStateStore ::
  (Typeable k, Typeable v) =>
  StateStore k v ->
  EStateStore
wrapStateStore stateStore =
  case stateStore of
    KVStateStore kvStore -> EKVStateStore (DEKVStore kvStore)
    SessionStateStore sessionStore -> ESessionStateStore (DESessionStore sessionStore)

class SessionStore s where
  ssGet :: (Typeable k, Ord k) => SessionWindowKey k -> s k v -> IO (Maybe v)
  ssPut :: (Typeable k, Ord k) => SessionWindowKey k -> v -> s k v -> IO ()
  ssRemove :: (Typeable k, Ord k) => SessionWindowKey k -> s k v -> IO ()
  findSessions :: (Typeable k, Ord k) => k -> Timestamp -> Timestamp -> s k v -> IO [(SessionWindowKey k, v)]

data ESessionStore k v
  = forall s.
    SessionStore s =>
    ESessionStore (s k v)

instance SessionStore ESessionStore where
  ssGet k (ESessionStore s) = ssGet k s

  ssPut k v (ESessionStore s) = ssPut k v s

  ssRemove k (ESessionStore s) = ssRemove k s

  findSessions k ts1 ts2 (ESessionStore s) = findSessions k ts1 ts2 s

data DESessionStore
  = forall k v.
    (Typeable k, Typeable v) =>
    DESessionStore (ESessionStore k v)

fromDESessionStoreToESessionStore ::
  (Typeable k, Typeable v, Ord k) =>
  DESessionStore ->
  ESessionStore k v
fromDESessionStoreToESessionStore (DESessionStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing ->
      throw $
        TypeCastError $
          "fromDESessionStoreToESessionStore: type cast error, actual eStore type is " `T.append` T.pack (show $ typeOf eStore)

data InMemorySessionStore k v = InMemorySessionStore
  { imssData :: IORef (Map Timestamp (IORef (Map k (IORef (Map Timestamp v)))))
  }

mkInMemorySessionStore :: IO (InMemorySessionStore k v)
mkInMemorySessionStore = do
  internalData <- newIORef Map.empty
  return
    InMemorySessionStore
      { imssData = internalData
      }

instance SessionStore InMemorySessionStore where
  ssGet TimeWindowKey {..} InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            return $ Map.lookup ws dict2
          Nothing -> return Nothing
      Nothing -> return Nothing

  ssPut TimeWindowKey {..} v InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            writeIORef rd1 (Map.insert ws v dict2)
          Nothing -> do
            rd1 <- newIORef $ Map.singleton ws v
            writeIORef rd (Map.insert twkKey rd1 dict1)
      Nothing -> do
        rd1 <- newIORef $ Map.singleton ws v
        rd2 <- newIORef $ Map.singleton twkKey rd1
        writeIORef imssData (Map.insert we rd2 dict0)

  ssRemove TimeWindowKey {..} InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            writeIORef rd1 (Map.delete ws dict2)
          Nothing -> return ()
      Nothing -> return ()

  findSessions key earliestSessionEndTime latestSessionStartTime InMemorySessionStore {..} = do
    dict0 <- readIORef imssData
    let (_, mv0, tailDict0') = Map.splitLookup earliestSessionEndTime dict0
    let tailDict0 =
          if isJust mv0
            then Map.insert earliestSessionEndTime (fromJust mv0) tailDict0'
            else tailDict0'
    Map.foldlWithKey'
      ( \macc et rd1 -> do
          acc <- macc
          dict1 <- readIORef rd1
          case Map.lookup key dict1 of
            Just rd2 -> do
              dict2 <- readIORef rd2
              let (headDict2', mv2, _) = Map.splitLookup latestSessionStartTime dict2
              let headDict2 =
                    if isJust mv2
                      then Map.insert latestSessionStartTime (fromJust mv2) headDict2'
                      else headDict2'
              return $
                Map.foldlWithKey'
                  ( \acc1 st v ->
                      acc1 ++ [(mkTimeWindowKey key (mkTimeWindow st et), v)]
                  )
                  acc
                  headDict2
            Nothing -> return acc
      )
      (return [])
      tailDict0

mkInMemoryStateKVStore :: IO (StateStore k v)
mkInMemoryStateKVStore = do
  store <- mkInMemoryKVStore
  return $ KVStateStore $ EKVStore store

mkInMemoryStateSessionStore :: IO (StateStore k v)
mkInMemoryStateSessionStore = do
  store <- mkInMemorySessionStore
  return $ SessionStateStore $ ESessionStore store

fromEStateStoreToKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  EStateStore ->
  EKVStore k v
fromEStateStoreToKVStore eStore =
  case eStore of
    EKVStateStore s -> fromDEKVStoreToEKVStore s
    _ -> throw $ UnExpectedStateStoreType "expect KVStateStore"

fromEStateStoreToSessionStore ::
  (Typeable k, Typeable v, Ord k) =>
  EStateStore ->
  ESessionStore k v
fromEStateStoreToSessionStore eStore =
  case eStore of
    ESessionStateStore s -> fromDESessionStoreToESessionStore s
    _ -> throw $ UnExpectedStateStoreType "expect SessionStateStore"

--
-- 用 ADT 就会造成匹配要修改？
-- 还好吧，
-- 只要各取所需应该就可以.
--
--
-- Store 之间是否要有类似继承的关系？
-- SessionStore 其实就是一种特殊的 KVStore,
-- 如果用继承来建模的话，
-- addStateStore 那里如果用了 KVStore 做签名，
-- 然而实际传进去的是一个 SessionStore,
-- 我还能用 findSessions 方法吗？
-- 理论上不能，
-- 但是 oop 里面可以进行 cast，将父类型强转成子类型.
-- 如果不能用强制 cast, 继承就没啥好处.
--
-- 那如果将这两个接口分开，相互独立，
-- 又有哪些问题呢？
-- 就是写 addStateStore 的时候签名给什么呢？
-- 可能需要构造一个 ADT 了，
--
-- 这种问题又是来源于那种想用一个概念抽象一组东西，
-- 然后用这个抽象的概念作为统一的接口，
-- 然而实现上却要依赖各自实现的不同接口.
-- 这里你要用到的是特化的部分，
-- 而不是一致的部分.
-- 所以这种抽象在这种情况下感觉不是很好。
--
-- ADT 呢就相当于把这些特殊情况事先都列举出来了，
-- 缺点就是代码修改起来会很麻烦。
-- ADT 里加一项，
-- 所有匹配的地方就都要跟着加.
--
-- 如果现在换成 ADT 的话，
-- 之前用 KVStore 的地方，
-- 在使用前都要先做一下模式匹配，
-- 但模式匹配用在这里也不用很合适，
-- 模式匹配更适合于那种多种情况都有可能发生，
-- 每个模式代表一种可能的情况，
-- 然后代码要能处理这些所有的情况.
--
-- 现在这种情况其实是要求特事特办，
-- 我要用 KVStore 的地方，
-- 不希望你传给我 SessionStore,
-- 反之我要 SessionStore 的地方，
-- 你也不要只给我 KVStore.
--
-- 但是不管怎样呢，
-- 我还是需要用一个变量表示这两种不同的 Store,
-- 就像 Java  需要向下 cast 一样，
-- 这是跑不掉的.
--
-- 这就是就是 Expression Problem.
--
-- 比如一开始有个 data Shape
-- 覆盖了正方形和圆形两种 shape,
-- 有对 shape 求面积的方法.
--
-- 现在要加一个三角形和一个求周长的方法进来，
-- 应该如何在不影响之前代码的情况下实现呢？
--
-- 还有没有别的方法进行建模呢?
