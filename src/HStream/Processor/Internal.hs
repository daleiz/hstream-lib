{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Processor.Internal
  ( TaskBuilder,
    TaskTopologyConfig (..),
    TaskContext (..),
    Task (..),
    InternalSourceConfig (..),
    InternalSinkConfig (..),
    buildTaskContext,
    isSink,
  )
where

import Control.Comonad.Traced
import Control.Exception (throw)
import Data.Default
import Data.Dynamic
import HStream.Error (HStreamError (..))
import RIO
import qualified RIO.HashMap as HM
import qualified RIO.Text as T

data TaskTopologyConfig = TaskTopologyConfig
  { sourceCfgs :: HM.HashMap T.Text InternalSourceConfig,
    topology :: HM.HashMap T.Text (Dynamic, [T.Text]),
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
    iSourceTopicName :: T.Text,
    iKeyDeserializer :: Maybe Dynamic,
    iValueDeserializer :: Dynamic
  }
  deriving (Show)

data InternalSinkConfig = InternalSinkConfig
  { iSinkName :: T.Text,
    iSinkTopicName :: T.Text,
    iKeySerializer :: Maybe Dynamic,
    iValueSerializer :: Dynamic
  }
  deriving (Show)

type TaskBuilder = Traced TaskTopologyConfig Task

data Task = Task
  { taskName :: T.Text,
    taskSourceConfig :: HM.HashMap T.Text InternalSourceConfig,
    taskTopologyReversed :: HM.HashMap T.Text (Dynamic, [T.Text]),
    taskTopologyForward :: HM.HashMap T.Text (Dynamic, [T.Text]),
    taskSinkConfig :: HM.HashMap T.Text InternalSinkConfig
  }
  deriving (Show)

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

isSink :: HashMap T.Text InternalSinkConfig -> T.Text -> Bool
isSink cfgs curName = HM.member curName cfgs
