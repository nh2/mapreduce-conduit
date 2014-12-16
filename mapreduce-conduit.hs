{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, GADTs, StandaloneDeriving, FlexibleContexts, ScopedTypeVariables, RankNTypes, DataKinds, TypeOperators, KindSignatures, FlexibleInstances, MultiWayIf #-}

module MapreduceConduit where

import           Control.Monad
import           Control.Applicative
import           Control.Exception
import           Control.Monad.IO.Class (liftIO)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Conduit
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network
import           Data.List (intersperse)
import           Safe (atMay)
import           Data.Serialize
import           Data.Typeable
import           Data.Void
import           Text.Read (readMaybe)

import           System.Environment



-- TODO The ByteStrings we want to deserialise from bsSource can likely be
--      split across multiple ByteStrings since it just calls Unix's `recv`
--      inside. We will likely have to buffer and send a size first.

runNetworkConduit :: (Serialize i, Serialize o)
                  => ServerSettings -> ClientSettings -> Conduit i IO o -> IO ()
runNetworkConduit ss cs c = do
  runTCPServer ss $ \inConn -> do
    liftIO $ putStrLn "Client connected"
    runTCPClient cs $ \outConn -> do -- TODO We don't want to open the conn for each of our clients
      let bsSource = appSource inConn
      let bsSink   = appSink   outConn
      liftIO $ putStrLn "Connected to server"

      bsSource $$ C.mapM decodeOrThrow $= c $= C.map encode $= bsSink


runNetworkSource :: (Serialize o) => ClientSettings -> Source IO o -> IO ()
runNetworkSource cs source = do
  runTCPClient cs $ \outConn -> do
    liftIO $ putStrLn "Connected to server"
    liftIO $ print (appSockAddr outConn)
    source $$ C.map encode $= appSink outConn


runNetworkSink :: (Serialize i) => ServerSettings -> Sink i IO () -> IO ()
runNetworkSink ss sink = do
  runTCPServer ss $ \inConn -> do
    liftIO $ putStrLn "Client connected"
    appSource inConn $$ C.mapM decodeOrThrow $= sink



decodeOrThrow :: (Serialize a) => ByteString -> IO a
decodeOrThrow bs = case decode bs of
  Left err  -> throwIO $ DeserializeException err
  Right x   -> return x



data DeserializeException = DeserializeException String
  deriving (Eq, Ord, Show, Typeable)

instance Exception DeserializeException


data Pipeline :: (* -> *) -> [*] -> * where

  End
    :: { endConduit :: ConduitM i Void m r
       }
    -> Pipeline m '[i,()]

  Step
    :: { stepConduit :: ConduitM i o m ()
       , tailPipeline :: Pipeline m (o ': rest)
       }
    -> Pipeline m (i ': o ': rest)


(---->) :: ConduitM i o m () -> Pipeline m (o ': rest) -> Pipeline m (i ': o ': rest)
c ----> pipeline = Step c pipeline

infixr 2 ---->


class PipelineTypes a where
  pipelineTypes :: a -> [TypeRep]

instance (Typeable i, Typeable o) => PipelineTypes (Pipeline m '[i,o]) where
  pipelineTypes _ = [typeOf (undefined :: i), typeOf (undefined :: o)]
instance (Typeable i, PipelineTypes (Pipeline m (x ': y ': rest))) => PipelineTypes (Pipeline m (i ': x ': y ': rest)) where
  pipelineTypes (Step _ rest) = typeOf (undefined :: i) : pipelineTypes rest


-- TODO: Can we do the Show instance without UndecidableInstances?
-- instance (PipelineTypes (Pipeline m types)) => Show (Pipeline m types) where
--   show x = "Pipeline [" ++ concat (intersperse "," (map show (pipelineTypes x))) ++ "]"

showPipeline :: (PipelineTypes a) => a -> String
showPipeline p = "Pipeline [" ++ concat (intersperse "," (map show (pipelineTypes p))) ++ "]"


infixr `Step`




type PortNumber = Int


class RunPipelineConduit pipeline where
  runPipelineConduit :: Int -> (PortNumber, PortNumber) -> pipeline -> IO ()

instance RunPipelineConduit (Pipeline IO '[a,b,c]) where
  runPipelineConduit 0 _ _ = error "runPipelineConduit: cannot start a sink; use runPipelineSink"
  runPipelineConduit n _ _ = error $ "runPipelineConduit: out of bounds (3): " ++ show n
instance (Serialize i, Serialize o,
          RunPipelineConduit (Pipeline IO (i ': o ': fill ': rest)))
         => RunPipelineConduit (Pipeline IO (pre ': i ': o ': fill ': rest)) where
  runPipelineConduit 1 (iPort, oPort) p = case p of
    Step _ (Step c _) -> do
      putStrLn $ "Running conduit at " ++ show iPort ++ " to " ++ show oPort
      runNetworkConduit (serverSettings iPort "*") (clientSettings oPort "localhost") c
    -- If we only use the pattern above (which is the only possible one),
    -- GHC 7.8 gives a spurious Patterns not matched: Step _ (End _), see
    --   https://ghc.haskell.org/trac/ghc/ticket/3927
    -- So we have to add this one:
    _ -> error "runPipelineConduit: cannot happen"
  runPipelineConduit n ports (Step _ rest)
    | n == 0 = error "runPipelineConduit: cannot start a source; use runPipelineSource"
    | n <  0 = error "runPipelineConduit: negative index"
    | otherwise = runPipelineConduit (n-1) ports rest

runPipelineSource :: (Serialize o) => PortNumber -> Pipeline IO (() ': o ': rest) -> IO ()
runPipelineSource oPort p = case p of
  -- TODO: We can probably prevent that on the type level by requiring pipelines
  --       to have at least 3 entries.
  End{}    -> error "runPipelineSource: pipline has only 1 entry"
  Step c _ -> do
    putStrLn $ "Running source at " ++ show oPort
    runNetworkSource (clientSettings oPort "localhost") c


class RunPipelineSink pipeline where
  runPipelineSink :: PortNumber -> pipeline -> IO ()

instance RunPipelineSink (Pipeline IO '[x,y]) where
  runPipelineSink _ _ = error "runPipelineSink: pipeline has only 1 entry"
instance (Serialize i) => RunPipelineSink (Pipeline IO '[x,i,()]) where
  runPipelineSink iPort (Step _ (End c)) = do
    putStrLn $ "Running sink at " ++ show iPort
    runNetworkSink (serverSettings iPort "*") (void c)
  -- TODO: Why does GHC suggest I need this? It doesn't allow me to put `Step _ _` or `End _` for `_x`!
  --       And I also don't think the `Step _ (Step _ _)` case is possible at all.
  --       See https://ghc.haskell.org/trac/ghc/ticket/3927
  runPipelineSink _     (Step _ (Step _ _x)) = error "runPipelineSink: cannot happen"
instance (RunPipelineSink (Pipeline IO (fill1 ': fill2 ': fill3 ': rest))) => RunPipelineSink (Pipeline IO (x ': fill1 ': fill2 ': fill3 ': rest)) where
  runPipelineSink iPort (Step _ rest) = runPipelineSink iPort rest


main :: IO ()
main = do
  let inPort  i = serverSettings i "*"
      outPort o = clientSettings o "localhost"

  args <- getArgs
  case args of

    ["BS.length", iPortStr, oPortStr]
      | Just i <- inPort  <$> readMaybe iPortStr
      , Just o <- outPort <$> readMaybe oPortStr -> runNetworkConduit i o $ do

          awaitForever $ \bs ->
            yield (BS.length bs)

              :: Conduit ByteString IO Int

    ["BS.getLine", oPortStr]
      | Just o <- outPort <$> readMaybe oPortStr -> runNetworkSource o $ do

          C.repeatM BS.getLine

            :: Source IO ByteString

    ["BS.putStrLn", iPortStr]
      | Just i <- inPort <$> readMaybe iPortStr -> runNetworkSink i $ do

          awaitForever $ liftIO . print

            :: Sink Int IO ()

    ["length-pipeline", iStr]
      | Just i <- readMaybe iStr -> do

          let pipeline :: Pipeline IO '[(), ByteString, Int, ()]
              pipeline =
                (C.repeatM BS.getLine)
                ---->
                (awaitForever $ \bs -> yield (BS.length bs))
                ---->
                End (awaitForever (liftIO . print))

          putStrLn $ showPipeline pipeline -- prints all types in the pipeline

          let n = length (pipelineTypes pipeline) - 1 -- TODO: This is just lazy
              portRange = take (n-1) [10000..]

          runPipelineIndex pipeline i ( portRange `atMay` (i-1)
                                      , portRange `atMay` i
                                      )
    _ ->
      error "bad arguments"


-- Runs a pipeline that has at least one conduit inside.
runPipelineIndex :: (PipelineTypes (Pipeline IO (() ': o ': rest)),
                     RunPipelineConduit (Pipeline IO (() ': o ': rest)),
                     RunPipelineSink (Pipeline IO (() ': o ': rest)), Serialize o)
                 => Pipeline IO (() ': o ': rest)
                 -> Int
                 -> (Maybe PortNumber, Maybe PortNumber)
                 -> IO ()
runPipelineIndex pipeline i ports = if
  -- Bad index
  | i < 0    -> error "cannot run pipeline step with negative index"
  | i >= n   -> error "pipeline step index exceeds pipeline length"

  -- Source
  | i == 0 -> case ports of
      (Nothing, Just port) -> runPipelineSource port pipeline
      _                    -> error $ "runPipelineIndex: bad source ports: " ++ show ports

  -- Sink
  | i == n-1 -> case ports of
      (Just port, Nothing) -> runPipelineSink port pipeline
      _                    -> error $ "runPipelineIndex: bad sink ports: " ++ show ports

  -- In between: Conduit
  | (Just iPort, Just oPort) <- ports -> runPipelineConduit i (iPort, oPort) pipeline
  | otherwise                         -> error $ "runPipelineIndex: bad conduit ports: " ++ show ports
  where
    n = length (pipelineTypes pipeline) - 1 -- TODO: This is just lazy
