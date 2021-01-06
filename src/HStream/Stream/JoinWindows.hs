{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoImplicitPrelude #-}

module HStream.Stream.JoinWindows
  ( JoinWindows (..),
  )
where

import RIO

data JoinWindows = JoinWindows
  { jwBeforeMs :: Int64,
    jwAfterMs :: Int64,
    jwGraceMs :: Int64
  }
