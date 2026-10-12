module SSE.Parser
  ( EventStreamParser
  , initialEventStreamParser
  , feedEventStreamParser
  , eventStreamParserStreamState
  , foldServerEvents
  , Step (Continue, Stop)
  , ServerEvent (ServerEvent, serverEventType, serverEventData, serverEventLastEventId)
  , ReconnectionTime (ReconnectionTime, reconnectionTimeMilliseconds)
  , EventStreamState
  , eventStreamStateLastEventId
  , eventStreamStateReconnectionTime
  ) where

import Data.ByteString qualified as BS
import Data.List qualified as List
import Data.Maybe qualified as Maybe
import Data.Text qualified as T
import Data.Text.Encoding qualified as Enc
import Data.Text.Encoding.Error qualified as EncError
import Data.Text.Read qualified as TR
import Data.Word (Word8)

data EventStreamParser = EventStreamParser
  { eventStreamParserLineBuffer :: !EventStreamLineBuffer
  , eventStreamParserStreamState :: !EventStreamState
  }

initialEventStreamParser :: EventStreamParser
initialEventStreamParser =
  EventStreamParser
    { eventStreamParserLineBuffer = emptyEventStreamLineBuffer
    , eventStreamParserStreamState = initialEventStreamState
    }

feedEventStreamParser :: BS.ByteString -> EventStreamParser -> ([ServerEvent], EventStreamParser)
feedEventStreamParser chunk parser =
  let
    (completedLines, lineBuffer) =
      feedEventStreamChunk chunk (eventStreamParserLineBuffer parser)

    processLine (state, events) line =
      let
        (!nextState, mbEvent) = processEventStreamLine (parseEventStreamLine line) state
       in
        (nextState, Maybe.maybe events (: events) mbEvent)

    (streamState, reversedEvents) =
      List.foldl' processLine (eventStreamParserStreamState parser, []) completedLines
   in
    ( reverse reversedEvents
    , EventStreamParser
        { eventStreamParserLineBuffer = lineBuffer
        , eventStreamParserStreamState = streamState
        }
    )

data Step a
  = Continue a
  | Stop a

foldServerEvents ::
  (a -> ServerEvent -> IO (Step a)) ->
  a ->
  IO BS.ByteString ->
  IO (a, EventStreamState)
foldServerEvents handleEvent initial readChunk =
  let
    readNextChunk acc parser = do
      chunk <- readChunk
      if BS.null chunk
        then pure (acc, eventStreamParserStreamState parser)
        else case feedEventStreamParser chunk parser of
          (events, !nextParser) -> deliverEvents acc nextParser events

    deliverEvents acc parser events =
      case events of
        [] ->
          readNextChunk acc parser
        event : rest -> do
          step <- handleEvent acc event
          case step of
            Continue !nextAcc -> deliverEvents nextAcc parser rest
            Stop finalAcc -> pure (finalAcc, eventStreamParserStreamState parser)
   in
    readNextChunk initial initialEventStreamParser

data EventStreamState = EventStreamState
  { eventStreamStateDataBuffer :: ![T.Text]
  , eventStreamStateEventTypeBuffer :: !T.Text
  , eventStreamStateLastEventIdBuffer :: !T.Text
  , eventStreamStateLastEventId :: !T.Text
  , eventStreamStateReconnectionTime :: !(Maybe ReconnectionTime)
  }

initialEventStreamState :: EventStreamState
initialEventStreamState =
  EventStreamState
    { eventStreamStateDataBuffer = []
    , eventStreamStateEventTypeBuffer = ""
    , eventStreamStateLastEventIdBuffer = ""
    , eventStreamStateLastEventId = ""
    , eventStreamStateReconnectionTime = Nothing
    }

newtype ReconnectionTime = ReconnectionTime
  { reconnectionTimeMilliseconds :: Integer
  }
  deriving (Eq, Show)

parseReconnectionTime :: T.Text -> Maybe ReconnectionTime
parseReconnectionTime value =
  case TR.decimal value of
    Right (millis, rest)
      | T.null rest ->
          Just (ReconnectionTime millis)
    _ ->
      Nothing

data ServerEvent = ServerEvent
  { serverEventType :: T.Text
  , serverEventData :: T.Text
  , serverEventLastEventId :: T.Text
  }
  deriving (Eq, Show)

processEventStreamLine ::
  EventStreamLine ->
  EventStreamState ->
  (EventStreamState, Maybe ServerEvent)
processEventStreamLine line state =
  case line of
    EventStreamBlankLine -> dispatchEvent state
    EventStreamComment _ -> (state, Nothing)
    EventStreamField name value -> (processField name value state, Nothing)

processField :: EventStreamFieldName -> T.Text -> EventStreamState -> EventStreamState
processField name value state =
  case name of
    EventTypeField ->
      state{eventStreamStateEventTypeBuffer = value}
    DataField ->
      state{eventStreamStateDataBuffer = value : eventStreamStateDataBuffer state}
    IdField
      | not (T.elem '\NUL' value) ->
          state{eventStreamStateLastEventIdBuffer = value}
    RetryField
      | Just time <- parseReconnectionTime value ->
          state{eventStreamStateReconnectionTime = Just time}
    _ ->
      state

dispatchEvent :: EventStreamState -> (EventStreamState, Maybe ServerEvent)
dispatchEvent state =
  let
    lastEventId =
      eventStreamStateLastEventIdBuffer state

    resetState =
      state
        { eventStreamStateDataBuffer = []
        , eventStreamStateEventTypeBuffer = ""
        , eventStreamStateLastEventId = lastEventId
        }

    eventTypeBuffer =
      eventStreamStateEventTypeBuffer state

    eventType =
      if T.null eventTypeBuffer
        then "message"
        else eventTypeBuffer
   in
    case eventStreamStateDataBuffer state of
      [] ->
        (resetState, Nothing)
      dataLines ->
        ( resetState
        , Just
            ServerEvent
              { serverEventType = eventType
              , serverEventData = T.intercalate "\n" (reverse dataLines)
              , serverEventLastEventId = lastEventId
              }
        )

data EventStreamLine
  = EventStreamBlankLine
  | EventStreamComment T.Text
  | EventStreamField EventStreamFieldName T.Text
  deriving (Eq, Show)

parseEventStreamLine :: T.Text -> EventStreamLine
parseEventStreamLine line =
  case T.break (== ':') line of
    ("", "") ->
      EventStreamBlankLine
    ("", afterName) ->
      EventStreamComment (T.drop 1 afterName)
    (name, afterName) ->
      let
        value = T.drop 1 afterName
       in
        EventStreamField (parseEventStreamFieldName name) (Maybe.fromMaybe value (T.stripPrefix " " value))

data EventStreamFieldName
  = EventTypeField
  | DataField
  | IdField
  | RetryField
  | UnknownField T.Text
  deriving (Eq, Show)

parseEventStreamFieldName :: T.Text -> EventStreamFieldName
parseEventStreamFieldName name =
  case name of
    "event" -> EventTypeField
    "data" -> DataField
    "id" -> IdField
    "retry" -> RetryField
    _ -> UnknownField name

data EventStreamLineBuffer = EventStreamLineBuffer
  { eventStreamLineBufferPartialLine :: ![BS.ByteString]
  , eventStreamLineBufferPrecedingByte :: !PrecedingByte
  , eventStreamLineBufferPosition :: !StreamPosition
  }

data PrecedingByte
  = PrecededByCarriageReturn
  | NotPrecededByCarriageReturn

data StreamPosition
  = StreamStart
  | PastStreamStart

emptyEventStreamLineBuffer :: EventStreamLineBuffer
emptyEventStreamLineBuffer =
  EventStreamLineBuffer
    { eventStreamLineBufferPartialLine = []
    , eventStreamLineBufferPrecedingByte = NotPrecededByCarriageReturn
    , eventStreamLineBufferPosition = StreamStart
    }

feedEventStreamChunk ::
  BS.ByteString ->
  EventStreamLineBuffer ->
  ([T.Text], EventStreamLineBuffer)
feedEventStreamChunk chunk initialBuffer =
  let
    go completedLines buffer bytes =
      if BS.null bytes
        then (reverse completedLines, buffer)
        else
          let
            (beforeEnding, fromEnding) =
              BS.break isLineEnding (dropLineFeedAfterCarriageReturn (eventStreamLineBufferPrecedingByte buffer) bytes)

            partialLine =
              beforeEnding : eventStreamLineBufferPartialLine buffer
           in
            case BS.uncons fromEnding of
              Nothing ->
                ( reverse completedLines
                , buffer
                    { eventStreamLineBufferPartialLine = partialLine
                    , eventStreamLineBufferPrecedingByte = NotPrecededByCarriageReturn
                    }
                )
              Just (ending, afterEnding) ->
                go
                  (finishLine (eventStreamLineBufferPosition buffer) partialLine : completedLines)
                  buffer
                    { eventStreamLineBufferPartialLine = []
                    , eventStreamLineBufferPrecedingByte =
                        if ending == carriageReturnByte
                          then PrecededByCarriageReturn
                          else NotPrecededByCarriageReturn
                    , eventStreamLineBufferPosition = PastStreamStart
                    }
                  afterEnding
   in
    go [] initialBuffer chunk

dropLineFeedAfterCarriageReturn :: PrecedingByte -> BS.ByteString -> BS.ByteString
dropLineFeedAfterCarriageReturn precedingByte bytes =
  case (precedingByte, BS.uncons bytes) of
    (PrecededByCarriageReturn, Just (firstByte, afterFirstByte))
      | firstByte == lineFeedByte -> afterFirstByte
    _ -> bytes

isLineEnding :: Word8 -> Bool
isLineEnding byte =
  byte == lineFeedByte || byte == carriageReturnByte

lineFeedByte :: Word8
lineFeedByte =
  0x0A

carriageReturnByte :: Word8
carriageReturnByte =
  0x0D

finishLine :: StreamPosition -> [BS.ByteString] -> T.Text
finishLine position partialLine =
  let
    lineBytes =
      BS.concat (reverse partialLine)

    withoutByteOrderMark =
      case position of
        StreamStart -> Maybe.fromMaybe lineBytes (BS.stripPrefix byteOrderMark lineBytes)
        PastStreamStart -> lineBytes
   in
    Enc.decodeUtf8With EncError.lenientDecode withoutByteOrderMark

byteOrderMark :: BS.ByteString
byteOrderMark =
  BS.pack [0xEF, 0xBB, 0xBF]
