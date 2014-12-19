{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, GADTs, StandaloneDeriving, FlexibleContexts, ScopedTypeVariables, RankNTypes, DataKinds, TypeOperators, KindSignatures, FlexibleInstances, MultiWayIf, NamedFieldPuns, ParallelListComp #-}

module MapreduceConduit where

import           Control.Applicative
import           Control.Concurrent.Async (mapConcurrently)
import           Control.Monad
import           Control.Monad.IO.Class (MonadIO)
import           Control.Exception
import           Control.Monad.IO.Class (liftIO)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import           Data.Conduit
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network
import           Data.List (intersperse)
import qualified Data.Map as Map
import           Safe (atMay)
import           Data.Serialize
import           Data.Typeable
import           Data.Void
import           Text.Read (readMaybe)

import           System.Environment



sendConduit :: (Serialize i, MonadIO m) => (i -> Int) -> [ClientAddr] -> ConduitM i o m ()
sendConduit hashFun oAddrs = awaitForever $ \o -> liftIO $ do
  let oAddr = oAddrs !! (hashFun o `mod` length oAddrs)
  runTCPClient (unClientAddr oAddr) $ \outConn -> do -- TODO We don't want to open the conn for each of our clients
    let bsSink = appSink outConn
    liftIO . putStrLn $ "Connected to server " ++ show oAddr
    yield (encode o) $$ bsSink


-- TODO The ByteStrings we want to deserialise from bsSource can likely be
--      split across multiple ByteStrings since it just calls Unix's `recv`
--      inside. We will likely have to buffer and send a size first.

runNetworkConduit :: (Serialize i, Serialize o)
                  => ServerAddr -> (o -> Int) -> [ClientAddr] -> Conduit i IO o -> IO ()
runNetworkConduit iAddr hashFun oAddrs c = do
  runTCPServer (unServerAddr iAddr) $ \inConn -> do
    liftIO $ putStrLn $ "Client connected to " ++ show iAddr
    let bsSource = appSource inConn
    bsSource $$ C.mapM decodeOrThrow $= c $= sendConduit hashFun oAddrs


runNetworkSource :: (Serialize o) => (o -> Int) -> [ClientAddr] -> Source IO o -> IO ()
runNetworkSource hashFun oAddrs source = do
  source $$ sendConduit hashFun oAddrs


runNetworkSink :: (Serialize i) => ServerAddr -> Sink i IO () -> IO ()
runNetworkSink iAddr sink = do
  runTCPServer (unServerAddr iAddr) $ \inConn -> do
    liftIO $ putStrLn $ "Client connected to " ++ show iAddr
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
       , stepHashFun :: o -> Int
       , tailPipeline :: Pipeline m (o ': rest)
       }
    -> Pipeline m (i ': o ': rest)


(---->) :: ConduitM i o m () -> Pipeline m (o ': rest) -> Pipeline m (i ': o ': rest)
c ----> pipeline = Step c (const 0) pipeline

infixr 2 ---->


data StepParams i o m = StepParams
  { paramConduit :: ConduitM i o m ()
  , paramHashFun :: o -> Int
  }

withHashFun :: ConduitM i o m () -> (o -> Int) -> StepParams i o m
withHashFun = StepParams

(--->) :: StepParams i o m -> Pipeline m (o ': rest) -> Pipeline m (i ': o ': rest)
StepParams{ paramConduit, paramHashFun } ---> pipeline = Step paramConduit paramHashFun pipeline

infixr 2 --->



class PipelineTypes a where
  pipelineTypes :: a -> [TypeRep]

instance (Typeable i, Typeable o) => PipelineTypes (Pipeline m '[i,o]) where
  pipelineTypes _ = [typeOf (undefined :: i), typeOf (undefined :: o)]
instance (Typeable i, PipelineTypes (Pipeline m (x ': y ': rest))) => PipelineTypes (Pipeline m (i ': x ': y ': rest)) where
  pipelineTypes (Step _ _ rest) = typeOf (undefined :: i) : pipelineTypes rest


-- TODO: Can we do the Show instance without UndecidableInstances?
-- instance (PipelineTypes (Pipeline m types)) => Show (Pipeline m types) where
--   show x = "Pipeline [" ++ concat (intersperse "," (map show (pipelineTypes x))) ++ "]"

showPipeline :: (PipelineTypes a) => a -> String
showPipeline p = "Pipeline [" ++ concat (intersperse "," (map show (pipelineTypes p))) ++ "]"


infixr `Step`


-- TODO: This is just lazy
pipelineLength :: (PipelineTypes p) => p -> Int
pipelineLength pipeline = length (pipelineTypes pipeline) - 1


type PortNumber = Int


class RunPipelineConduit pipeline where
  runPipelineConduit :: Int -> ServerAddr -> [ClientAddr] -> pipeline -> IO ()

instance RunPipelineConduit (Pipeline IO '[a,b,c]) where
  runPipelineConduit 0 _ _ _ = error "runPipelineConduit: cannot start a sink; use runPipelineSink"
  runPipelineConduit n _ _ _ = error $ "runPipelineConduit: out of bounds (3): " ++ show n
instance (Serialize i, Serialize o,
          RunPipelineConduit (Pipeline IO (i ': o ': fill ': rest)))
         => RunPipelineConduit (Pipeline IO (pre ': i ': o ': fill ': rest)) where
  runPipelineConduit 1 iAddr oAddrs p = case p of
    Step _ _ (Step c hashFun _) -> do
      putStrLn $ "Running conduit at " ++ show iAddr ++ " to " ++ show oAddrs
      runNetworkConduit iAddr hashFun oAddrs c
    -- If we only use the pattern above (which is the only possible one),
    -- GHC 7.8 gives a spurious Patterns not matched: Step _ (End _), see
    --   https://ghc.haskell.org/trac/ghc/ticket/3927
    -- So we have to add this one:
    _ -> error "runPipelineConduit: cannot happen"
  runPipelineConduit n iAddr oAddrs (Step _ _ rest)
    | n == 0 = error "runPipelineConduit: cannot start a source; use runPipelineSource"
    | n <  0 = error "runPipelineConduit: negative index"
    | otherwise = runPipelineConduit (n-1) iAddr oAddrs rest

runPipelineSource :: (Serialize o) => [ClientAddr] -> Pipeline IO (() ': o ': rest) -> IO ()
runPipelineSource oAddrs p = case p of
  -- TODO: We can probably prevent that on the type level by requiring pipelines
  --       to have at least 3 entries.
  End{}            -> error "runPipelineSource: pipline has only 1 entry"
  Step c hashFun _ -> do
    putStrLn $ "Running source to " ++ show oAddrs
    runNetworkSource hashFun oAddrs c


class RunPipelineSink pipeline where
  runPipelineSink :: ServerAddr -> pipeline -> IO ()

instance RunPipelineSink (Pipeline IO '[x,y]) where
  runPipelineSink _ _ = error "runPipelineSink: pipeline has only 1 entry"
instance (Serialize i) => RunPipelineSink (Pipeline IO '[x,i,()]) where
  runPipelineSink iAddr (Step _ _ (End c)) = do
    putStrLn $ "Running sink at " ++ show iAddr
    runNetworkSink iAddr (void c)
  -- TODO: Why does GHC suggest I need this? It doesn't allow me to put `Step _ _` or `End _` for `_x`!
  --       And I also don't think the `Step _ (Step _ _)` case is possible at all.
  --       See https://ghc.haskell.org/trac/ghc/ticket/3927
  runPipelineSink _     (Step _ _ (Step _ _x _)) = error "runPipelineSink: cannot happen"
instance (RunPipelineSink (Pipeline IO (fill1 ': fill2 ': fill3 ': rest))) => RunPipelineSink (Pipeline IO (x ': fill1 ': fill2 ': fill3 ': rest)) where
  runPipelineSink iAddr (Step _ _ rest) = runPipelineSink iAddr rest


newtype ClientAddr = ClientAddr { unClientAddr :: ClientSettings }

instance Show ClientAddr where
  show (ClientAddr cs) = "ClientAddr " ++ BS8.unpack (getHost cs) ++ ":" ++ show (getPort cs)


newtype ServerAddr = ServerAddr { unServerAddr :: ServerSettings }

instance Show ServerAddr where
  show (ServerAddr ss) = "ServerAddr " ++ show (getPort ss)


type StageCounts = [Int]
type StageMap = Map.Map Int [Int]


mkStageMap :: StageCounts -> [PortNumber] -> StageMap
mkStageMap stageCounts ports = Map.fromListWith (flip (++)) stages -- TODO check if this is the bad-associative ++
  where
    -- TODO total tail
    stages = zip [ x | (s, c) <- zip [(0::Int)..] (tail stageCounts), x <- replicate c s ]
                 [ [p] | p <- ports ]


main :: IO ()
main = do
  let inAddr  i = ServerAddr $ serverSettings i "*"
      outAddr o = ClientAddr $ clientSettings o "localhost"
      hash0     = const 0

  args <- getArgs
  case args of

    ["BS.length", iPortStr, oPortStr]
      | Just i <- inAddr  <$> readMaybe iPortStr
      , Just o <- outAddr <$> readMaybe oPortStr -> runNetworkConduit i hash0 [o] $ do

          awaitForever $ \bs ->
            yield (BS.length bs)

              :: Conduit ByteString IO Int

    ["BS.getLine", oPortStr]
      | Just o <- outAddr <$> readMaybe oPortStr -> runNetworkSource hash0 [o] $ do

          C.repeatM BS.getLine

            :: Source IO ByteString

    ["BS.putStrLn", iPortStr]
      | Just i <- inAddr <$> readMaybe iPortStr -> runNetworkSink i $ do

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

          runPipelineIndexPortRange pipeline i portRange

    ["sharded-length-pipeline", iStr]
      | Just i <- readMaybe iStr -> do

          let firstCharHash :: ByteString -> Int
              firstCharHash bs
                | BS.null bs = 0
                | otherwise  = fromIntegral (BS.head bs) `rem` 26


          let pipeline :: Pipeline IO '[(), ByteString, Int, ()]
              pipeline =
                (C.repeatM BS.getLine) `withHashFun` firstCharHash
                --->
                (awaitForever $ \bs -> yield (BS.length bs))
                ---->
                End (awaitForever (liftIO . print))


          let stageCounts = [1, 3, 1] -- how many of each conduit to spawn
              stageMap    = mkStageMap stageCounts [10000..]

          putStrLn $ showPipeline pipeline -- prints all types in the pipeline
          print stageMap

          runPipelineIndexSharded pipeline i stageCounts stageMap

    _ ->
      error "bad arguments"


runPipelineIndexPortRange :: (Serialize o,
                              RunPipelineSink (Pipeline IO (() ': o ': rest)),
                              RunPipelineConduit (Pipeline IO (() ': o ': rest)),
                              PipelineTypes (Pipeline IO (() ': o ': rest)))
                          => Pipeline IO (() ': o ': rest)
                          -> Int
                          -> [Int]
                          -> IO ()
runPipelineIndexPortRange pipeline i portRange = runPipelineIndexSharded pipeline i stageCounts stageMap
  where
    stageCounts = replicate (pipelineLength pipeline) 1
    stageMap    = Map.fromList [ (x, [p]) | (x, p) <- zip [0..] portRange ]


-- Runs a pipeline that has at least one conduit inside.
runPipelineIndexSharded :: (Serialize o,
                            RunPipelineSink (Pipeline IO (() ': o ': rest)),
                            RunPipelineConduit (Pipeline IO (() ': o ': rest)),
                            PipelineTypes (Pipeline IO (() ': o ': rest)))
                        => Pipeline IO (() ': o ': rest)
                        -> Int
                        -> [Int]
                        -> Map.Map Int [Int]
                        -> IO ()
runPipelineIndexSharded pipeline i stageCounts stageMap = if
  -- Bad index
  | i < 0    -> error "cannot run pipeline step with negative index"
  | i >= n   -> error "pipeline step index exceeds pipeline length"

  -- Source
  | i == 0 -> case stageCounts `atMay` 0 of
      Just c  -> runAll [1..c] $ \_ -> runPipelineSource oAddrs pipeline
      Nothing -> error "runPipelineIndex: source has no stageCount"

  -- Sink
  | i == n-1 -> runAll iAddrs $ \iAddr -> runPipelineSink iAddr pipeline

  -- In between: Conduit
  | otherwise -> runAll iAddrs $ \iAddr -> runPipelineConduit i iAddr oAddrs pipeline
  where
    n = pipelineLength pipeline

    iAddrs = map (\p -> ServerAddr $ serverSettings p "*") $ case Map.lookup (i-1) stageMap of
               Nothing -> error "runPipelineIndex: iAddrs: out of range"
               Just ps -> ps

    oAddrs = map (\p -> ClientAddr $ clientSettings p "localhost") $ case Map.lookup i stageMap of
               Nothing -> error "runPipelineIndex: oAddrs: out of range"
               Just ps -> ps

    runAll :: [a] -> (a -> IO ()) -> IO ()
    runAll l f = void $ mapConcurrently f l
