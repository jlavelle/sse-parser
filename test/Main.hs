module Main (main) where

import Data.ByteString qualified as BS
import Data.Containers.ListUtils qualified as ListUtils
import Data.Foldable (fold)
import Data.IORef qualified as IORef
import Data.List qualified as List
import Data.List.NonEmpty qualified as NEL
import Data.Maybe qualified as Maybe
import Data.Text qualified as T
import Data.Text.Encoding qualified as Enc
import Hedgehog ((===))
import Hedgehog qualified as HH
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty qualified as Tasty
import Test.Tasty.Hedgehog qualified as TastyH

import SSE.Parser qualified as SSE

main :: IO ()
main =
  Tasty.defaultMain
    $ Tasty.testGroup
      "sse-parser"
      [ TastyH.testProperty "specification examples" specificationExamples
      , TastyH.testProperty "chunking and line endings" chunkingAndLineEndings
      , TastyH.testProperty "byte order mark only at stream start" byteOrderMarkOnlyAtStreamStart
      , TastyH.testProperty "empty data skips dispatch" emptyDataSkipsDispatch
      , TastyH.testProperty "id with NUL is ignored" idWithNullIsIgnored
      , TastyH.testProperty "retry" retry
      , TastyH.testProperty "UTF-8 decoding" utf8Decoding
      , TastyH.testProperty "stop ends the fold without reading further" stopEndsFold
      ]

specificationExamples :: HH.Property
specificationExamples =
  HH.withTests 1 . HH.property $ do
    ["data: YHOO\ndata: +2\ndata: 10\n\n"] `parsesTo` [messageEvent "YHOO\n+2\n10" ""]

    fourBlocks <-
      HH.evalIO . readAllEvents
        $ [": test stream\n\ndata: first event\nid: 1\n\ndata:second event\nid\n\ndata:  third event\n"]
    fst fourBlocks === [messageEvent "first event" "1", messageEvent "second event" ""]
    SSE.eventStreamStateLastEventId (snd fourBlocks) === ""

    ["data\n\ndata\ndata\n\ndata:"] `parsesTo` [messageEvent "" "", messageEvent "\n" ""]
    ["data:test\n\ndata: test\n\n"] `parsesTo` [messageEvent "test" "", messageEvent "test" ""]

data RenderedEvent = RenderedEvent
  { renderedEventType :: Maybe T.Text
  , renderedEventData :: T.Text
  , renderedEventId :: Maybe T.Text
  , renderedEventComment :: Maybe T.Text
  }
  deriving (Show)

chunkingAndLineEndings :: HH.Property
chunkingAndLineEndings =
  HH.property $ do
    events <- HH.forAll (Gen.list (Range.linear 0 5) genRenderedEvent)

    let
      streamLines =
        foldMap renderEventLines events

    lineEndings <-
      HH.forAll
        $ Gen.list
          (Range.singleton (length streamLines))
          (Gen.element ["\r\n", "\n", "\r"])

    let
      streamBytes =
        Enc.encodeUtf8
          . foldMap (\(line, ending) -> line <> ending)
          . resolveAmbiguousEndings
          $ zip streamLines lineEndings

      streamLength =
        BS.length streamBytes

      splitsCRLF cut =
        cut > 0
          && cut < streamLength
          && BS.index streamBytes (cut - 1) == 0x0D
          && BS.index streamBytes cut == 0x0A

      crlfSplitPoints =
        filter splitsCRLF [1 .. streamLength - 1]

      genCutPoint =
        Gen.choice
          $ Gen.int (Range.constant 0 streamLength)
            : Maybe.maybeToList (Gen.element <$> NEL.nonEmpty crlfSplitPoints)

    cutPoints <-
      HH.forAll . fmap (List.sort . ListUtils.nubOrd)
        $ Gen.list (Range.linear 0 8) genCutPoint

    let
      chunks =
        chunkAt cutPoints streamBytes

      expectedEvents =
        snd (List.mapAccumL expectedEvent "" events)

    HH.cover 20 "CRLF split across chunks" (any splitsCRLF cutPoints)
    HH.cover 20 "a lone CR line ending" ("\r" `elem` lineEndings)

    parsed <- HH.evalIO (readAllEvents chunks)
    fst parsed === expectedEvents

genRenderedEvent :: HH.Gen RenderedEvent
genRenderedEvent =
  RenderedEvent
    <$> Gen.maybe (Gen.text (Range.linear 1 8) genLineChar)
    <*> Gen.text (Range.linear 0 20) (Gen.choice [genLineChar, pure '\n'])
    <*> Gen.maybe (Gen.text (Range.linear 0 8) (Gen.filter (/= '\NUL') genLineChar))
    <*> Gen.maybe (Gen.text (Range.linear 0 8) genLineChar)

genLineChar :: HH.Gen Char
genLineChar =
  Gen.filter (\c -> c /= '\r' && c /= '\n') Gen.unicode

renderEventLines :: RenderedEvent -> [T.Text]
renderEventLines event =
  fold
    [ Maybe.maybeToList (fmap (":" <>) (renderedEventComment event))
    , Maybe.maybeToList (fmap ("event: " <>) (renderedEventType event))
    , fmap ("data: " <>) (T.splitOn "\n" (renderedEventData event))
    , Maybe.maybeToList (fmap ("id: " <>) (renderedEventId event))
    , [""]
    ]

expectedEvent :: T.Text -> RenderedEvent -> (T.Text, SSE.ServerEvent)
expectedEvent previousId event =
  let
    lastEventId =
      Maybe.fromMaybe previousId (renderedEventId event)
   in
    ( lastEventId
    , SSE.ServerEvent
        { SSE.serverEventType = Maybe.fromMaybe "message" (renderedEventType event)
        , SSE.serverEventData = renderedEventData event
        , SSE.serverEventLastEventId = lastEventId
        }
    )

resolveAmbiguousEndings :: [(T.Text, T.Text)] -> [(T.Text, T.Text)]
resolveAmbiguousEndings linesWithEndings =
  case linesWithEndings of
    (line, "\r") : ("", "\n") : rest ->
      (line, "\r") : resolveAmbiguousEndings (("", "\r\n") : rest)
    first : rest ->
      first : resolveAmbiguousEndings rest
    [] ->
      []

chunkAt :: [Int] -> BS.ByteString -> [BS.ByteString]
chunkAt cutPoints bytes =
  case cutPoints of
    [] -> [bytes]
    cut : cuts ->
      let
        (chunk, rest) = BS.splitAt cut bytes
       in
        chunk : chunkAt (fmap (subtract cut) cuts) rest

byteOrderMarkOnlyAtStreamStart :: HH.Property
byteOrderMarkOnlyAtStreamStart =
  HH.withTests 1 . HH.property $ do
    ["\xEF\xBB\xBF\&data: a\n\n\xEF\xBB\xBF\&data: b\n\n"] `parsesTo` [messageEvent "a" ""]
    ["\xEF", "\xBB\xBF\&data: a\n\n"] `parsesTo` [messageEvent "a" ""]

emptyDataSkipsDispatch :: HH.Property
emptyDataSkipsDispatch =
  HH.withTests 1 . HH.property $ do
    ["event: foo\nid: 1\n\ndata: x\n\nid: 2\n\ndata: y\n\n"] `parsesTo` [messageEvent "x" "1", messageEvent "y" "2"]

idWithNullIsIgnored :: HH.Property
idWithNullIsIgnored =
  HH.withTests 1 . HH.property $ do
    ["id: a\NUL\&b\ndata: x\n\n"] `parsesTo` [messageEvent "x" ""]

retry :: HH.Property
retry =
  HH.withTests 1 . HH.property $ do
    withInvalidValues <- HH.evalIO (readAllEvents ["retry: 1500\nretry: 10x\nretry:\n\n"])
    SSE.eventStreamStateReconnectionTime (snd withInvalidValues) === Just (SSE.ReconnectionTime 1500)

    large <- HH.evalIO (readAllEvents ["retry: 99999999999999999999\n"])
    SSE.eventStreamStateReconnectionTime (snd large) === Just (SSE.ReconnectionTime 99999999999999999999)

utf8Decoding :: HH.Property
utf8Decoding =
  HH.withTests 1 . HH.property $ do
    ["data: \xFF\n\n"] `parsesTo` [messageEvent "\xFFFD" ""]
    ["data: \xC3", "\xA9\n\n"] `parsesTo` [messageEvent "\xE9" ""]

stopEndsFold :: HH.Property
stopEndsFold =
  HH.withTests 1 . HH.property $ do
    chunksRef <- HH.evalIO (IORef.newIORef ["data: a\n\ndata: b\n\n", "data: c\n\n"])

    let
      stopAtB events event =
        pure
          $ if SSE.serverEventData event == "b"
            then SSE.Stop (event : events)
            else SSE.Continue (event : events)

    (events, _) <- HH.evalIO (SSE.foldServerEvents stopAtB [] (readChunkFrom chunksRef))
    remainingChunks <- HH.evalIO (IORef.readIORef chunksRef)
    fmap SSE.serverEventData events === ["b", "a"]
    remainingChunks === ["data: c\n\n"]

messageEvent :: T.Text -> T.Text -> SSE.ServerEvent
messageEvent eventData lastEventId =
  SSE.ServerEvent
    { SSE.serverEventType = "message"
    , SSE.serverEventData = eventData
    , SSE.serverEventLastEventId = lastEventId
    }

parsesTo :: [BS.ByteString] -> [SSE.ServerEvent] -> HH.PropertyT IO ()
parsesTo chunks expectedEvents = do
  parsed <- HH.evalIO (readAllEvents chunks)
  fst parsed === expectedEvents

readAllEvents :: [BS.ByteString] -> IO ([SSE.ServerEvent], SSE.EventStreamState)
readAllEvents chunks = do
  chunksRef <- IORef.newIORef (filter (not . BS.null) chunks)
  (reversedEvents, state) <-
    SSE.foldServerEvents (\events event -> pure (SSE.Continue (event : events))) [] (readChunkFrom chunksRef)
  pure (reverse reversedEvents, state)

readChunkFrom :: IORef.IORef [BS.ByteString] -> IO BS.ByteString
readChunkFrom chunksRef =
  IORef.atomicModifyIORef' chunksRef $ \remaining ->
    case remaining of
      [] -> ([], BS.empty)
      chunk : rest -> (rest, chunk)
