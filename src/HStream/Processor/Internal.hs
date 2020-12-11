{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Processor.Internal where

import Control.Comonad.Traced
import Control.Exception (throw)
import Data.Default
import Data.Typeable
import HStream.Error (HStreamError (..))
import RIO
import qualified RIO.HashMap as HM
import qualified RIO.Text as T

newtype Processor kin vin = Processor {runP :: Record kin vin -> RIO TaskContext ()}

newtype EProcessor = EProcessor {runEP :: ERecord -> RIO TaskContext ()}

mkEProcessor ::
  (Typeable k, Typeable v) =>
  Processor k v ->
  EProcessor
mkEProcessor proc = EProcessor $ \(ERecord record) ->
  case cast record of
    Just r -> runP proc r
    Nothing -> throw $ TypeCastError "mkEProcessor: type cast error"

data Record k v = Record
  { recordKey :: Maybe k,
    recordValue :: v
  }

data ERecord = forall k v. (Typeable k, Typeable v) => ERecord (Record k v)

mkERecord :: (Typeable k, Typeable v) => Record k v -> ERecord
mkERecord = ERecord

data TaskTopologyConfig = TaskTopologyConfig
  { sourceCfgs :: HM.HashMap T.Text InternalSourceConfig,
    topology :: HM.HashMap T.Text (EProcessor, [T.Text]),
    sinkCfgs :: HM.HashMap T.Text InternalSinkConfig
  }

instance Default TaskTopologyConfig where
  def =
    TaskTopologyConfig
      { sourceCfgs = HM.empty,
        topology = HM.empty,
        sinkCfgs = HM.empty
      }

instance Semigroup TaskTopologyConfig where
  t1 <> t2 =
    TaskTopologyConfig
      { sourceCfgs =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $ "source named " `T.append` name `T.append` " already existed"
            )
            (sourceCfgs t1)
            (sourceCfgs t2),
        topology =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $ "processor named " `T.append` name `T.append` " already existed"
            )
            (topology t1)
            (topology t2),
        sinkCfgs =
          HM.unionWithKey
            ( \name _ _ ->
                throw $
                  TaskTopologyBuildError $ "sink named " `T.append` name `T.append` " already existed"
            )
            (sinkCfgs t1)
            (sinkCfgs t2)
      }

instance Monoid TaskTopologyConfig where
  mempty = def

data InternalSourceConfig = InternalSourceConfig
  { iSourceName :: T.Text,
    iSourceTopicName :: T.Text
  }

data InternalSinkConfig = InternalSinkConfig
  { iSinkName :: T.Text,
    iSinkTopicName :: T.Text
  }

type TaskBuilder = Traced TaskTopologyConfig Task

data Task = Task
  { taskName :: T.Text,
    taskSourceConfig :: HM.HashMap T.Text InternalSourceConfig,
    taskTopologyReversed :: HM.HashMap T.Text (EProcessor, [T.Text]),
    taskTopologyForward :: HM.HashMap T.Text (EProcessor, [T.Text]),
    taskSinkConfig :: HM.HashMap T.Text InternalSinkConfig
  }

data TaskContext = TaskContext
  { taskConfig :: Task,
    tctLogFunc :: LogFunc,
    curProcessor :: IORef T.Text
  }

instance HasLogFunc TaskContext where
  logFuncL = lens tctLogFunc (\x y -> x {tctLogFunc = y})

buildTaskContext ::
  Task ->
  LogFunc ->
  IO TaskContext
buildTaskContext task lf = do
  ref <- newIORef ""
  return $
    TaskContext
      { taskConfig = task,
        tctLogFunc = lf,
        curProcessor = ref
      }
