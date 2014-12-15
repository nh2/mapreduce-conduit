{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, GADTs, StandaloneDeriving, FlexibleContexts, ScopedTypeVariables, RankNTypes, DataKinds, TypeOperators, KindSignatures, FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

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


-- Convenience hack until we write `runNetworkSink`.
instance Serialize Void

data ConduitPlan i o m r where
  Leaf :: ConduitM i o m r                                          -> ConduitPlan i o m r
  Node :: Typeable a => ConduitPlan i a m () -> ConduitPlan a o m r -> ConduitPlan i o m r


data Pipeline :: (* -> *) -> [*] -> * where
  End  :: ConduitM i Void m r  -> Pipeline m '[i,()]
  Step :: ConduitM i o    m () -> Pipeline m (o ': rest) -> Pipeline m (i ': o ': rest)


class PipelineTypes a where
  pipelineTypes :: a -> [TypeRep]

instance (Typeable i, Typeable o) => PipelineTypes (Pipeline m '[i,o]) where
  pipelineTypes _ = [typeOf (undefined :: i), typeOf (undefined :: o)]
instance (Typeable i, PipelineTypes (Pipeline m (x ': y ': rest))) => PipelineTypes (Pipeline m (i ': x ': y ': rest)) where
  pipelineTypes (Step _ rest) = typeOf (undefined :: i) : pipelineTypes rest


-- TODO: Can we do the Show instance without UndecidableInstances?
-- instance (PipelineTypes (Pipeline m types)) => Show (Pipeline m types) where
--   show x = "Plan [" ++ concat (intersperse "," (map show (pipelineTypes x))) ++ "]"

showPipeline :: (PipelineTypes a) => a -> String
showPipeline p = "Plan [" ++ concat (intersperse "," (map show (pipelineTypes p))) ++ "]"


infixr `Step`


instance Functor (ConduitPlan i o m) where
  fmap f (Leaf c) = Leaf (fmap f c)
  fmap f (Node l r) = Node l (fmap f r)


deriving instance Typeable ConduitM


instance (Typeable i, Typeable o, Typeable m, Typeable r, Typeable ConduitM) => Show (ConduitPlan i o m r) where
  show (p :: ConduitPlan i o m r) = "Plan " ++ show (planTypes p)


planTypes :: (Typeable i, Typeable o) => ConduitPlan i o m r -> [TypeRep]
planTypes p = case planTypeTuples p of
  []            -> []
  (t1, t2):rest -> t1 : t2 : map snd rest


planTypeTuples :: (Typeable i, Typeable o) => ConduitPlan i o m r -> [(TypeRep, TypeRep)]
planTypeTuples (Leaf{} :: ConduitPlan i o m r) = [(typeOf (undefined :: i), typeOf (undefined :: o))]
planTypeTuples (Node l r) = planTypeTuples l ++ planTypeTuples r


(--->) :: (Monad m, Typeable a) => ConduitPlan i a m () -> ConduitPlan a o m r -> ConduitPlan i o m r
(--->) = Node

infixl 2 --->


type PortNumber = Int


class RunPipelineConduit pipeline where
  runPipelineConduit :: Int -> [PortNumber] -> pipeline -> IO ()

instance RunPipelineConduit (Pipeline IO '[a,b,c]) where
  runPipelineConduit 0 _ _ = error "runPipelineConduit: cannot start a sink; use runPipelineSink"
  runPipelineConduit _ _ _ = error "runPipelineConduit: out of bounds"
instance (Serialize i, Serialize o,
          RunPipelineConduit (Pipeline IO (i ': o ': fill ': rest)))
         => RunPipelineConduit (Pipeline IO (pre ': i ': o ': fill ': rest)) where
  runPipelineConduit 1 (iPort:oPort:_) p = case p of
    Step _ (Step c _) -> do
      putStrLn $ "Running conduit at " ++ show iPort ++ " to " ++ show oPort
      runNetworkConduit (serverSettings iPort "*") (clientSettings oPort "localhost") c
    -- If we only use the pattern above (which is the only possible one),
    -- GHC 7.8 gives a spurious Patterns not matched: Step _ (End _), see
    --   https://ghc.haskell.org/trac/ghc/ticket/3927
    -- So we have to add this one:
    _ -> error "runPipelineConduit: cannot happen"
  runPipelineConduit n (_:ports) (Step _ rest)
    | n == 0 = error "runPipelineConduit: cannot start a source; use runPipelineSource"
    | n <  0 = error "runPipelineConduit: negative index"
    | otherwise = runPipelineConduit (n-1) ports rest
  runPipelineConduit _ [] _ = error "runPipelineConduit: out of ports"

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


-- class RunPipelineIndex pipeline where
--   runPipelineIndex :: Int -> [PortNumber] -> pipeline -> IO ()

-- instance RunPipelineIndex (Pipeline IO '[x,y]) where
--   runPipelineIndex _ _ _ = error "runPipelineIndex: pipeline has only 1 entry"
-- instance (Serialize a, Serialize b) => RunPipelineIndex (Pipeline IO '[a,b,()]) where
--   -- TODO: Why does GHC suggest I need this? It doesn't allow me to put `Step _ _` or `End _` for `_x`!
--   --       And I also don't think the `Step _ (Step _ _)` case is possible at all.
--   runPipelineIndex _ _                (Step _ (Step _ _)) = error "runPipelineIndex: cannot happen"
--   runPipelineIndex 0 (_:bPort:_)      (Step _ (End c))    = runNetworkSink (serverSettings bPort "*") (void c)
--   -- SOTA: Problem with this approach: if a is (), have to use runNetworkSource here.
--   --       But we can't check for this, that'd be overlapping instances of a and ().
--   --       Should probably continue with the 3 split functions approach above.
--   runPipelineIndex 1 (abPort:bPort:_) (Step c (End _))    = runNetworkConduit (serverSettings abPort "*") (clientSettings bPort "localhost") c
--   runPipelineIndex 0 _                (Step _ (End _))    = error "runPipelineIndex: out of ports"
--   runPipelineIndex n _                (Step _ (End _))
--     | n < 0                                               = error "runPipelineIndex: negative index"
--     | otherwise                                           = error "runPipelineIndex: out of bounds"

-- instance (Serialize i, Serialize o,
--           RunPipelineIndex (Pipeline IO (o ': fill1 ': fill2 ': rest)))
--          => RunPipelineIndex (Pipeline IO (i ': o ': fill1 ': fill2 ': rest)) where
--   runPipelineIndex 2 (iPort:oPort:_) p = case p of
--     Step c _ -> runNetworkConduit (serverSettings iPort "*") (clientSettings oPort "localhost") c
--   runPipelineIndex n (_:ports) (Step _ rest)
--     | n == 0 = error "runPipelineIndex: cannot start a source; use runPipelineSource"
--     | n <  0 = error "runPipelineIndex: negative index"
--     | otherwise = runPipelineIndex (n-1) ports rest
--   runPipelineIndex _ [] _ = error "runPipelineIndex: out of ports"


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

          -- let plan =
          --       Leaf (C.repeatM BS.getLine :: Source IO ByteString)
          --       --->
          --       Leaf (awaitForever $ \bs -> yield (BS.length bs))
          --       --->
          --       Leaf (awaitForever (liftIO . print) :: Sink Int IO ())

          -- print plan -- prints all types in the pipeline

          let pipeline :: Pipeline IO '[(), ByteString, Int, ()]
              pipeline =
                (C.repeatM BS.getLine)
                `Step`
                (awaitForever $ \bs -> yield (BS.length bs))
                `Step`
                End (awaitForever (liftIO . print))

          putStrLn $ showPipeline pipeline -- prints all types in the pipeline

          putStrLn $ "Running step number " ++ show (i :: Int)

          let ports = [10000..]
              n = length (pipelineTypes pipeline) - 1 -- TODO: This is just lazy
          putStrLn $ "Pipeline length is " ++ show n
          case i of
            0 -> runPipelineSource (head ports) pipeline
            _ | i < 0    -> error "cannot run pipeline step with negative index"
              | i >= n   -> error "pipeline step index exceeds pipeline length"
              | i == n-1 -> runPipelineSink (ports !! (i-1)) pipeline
            _ -> runPipelineConduit i ports pipeline

    _ ->
      error "bad arguments"
