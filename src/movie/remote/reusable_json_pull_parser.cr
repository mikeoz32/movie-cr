require "json"

module Movie::Remote::FrameCodec
  # Connection-local lexer that retains its token, key pool, and string buffer
  # across frames. The retained key pool is bounded so untrusted payload keys
  # cannot grow connection memory indefinitely. Reset intentionally mirrors
  # private JSON::Lexer::IOBased state from the supported Crystal 1.19-1.21
  # implementations; shard.yml has a matching upper bound so a new stdlib
  # layout must be reviewed before it is accepted.
  private class ReusableJsonLexer < JSON::Lexer::IOBased
    MAX_RETAINED_KEYS = 256

    def reset(io : IO) : self
      @io = io
      @line_number = 1_i64
      @column_number = 1_i64
      @buffer.clear
      @skip = false
      @expects_object_key = false
      @token.kind = :EOF
      @token.line_number = 0
      @token.column_number = 0
      @token.string_value = ""
      @token.raw_value = ""
      @current_char = @io.read_char || '\0'
      self
    end

    def release_excess(discard_buffer : Bool) : Nil
      @buffer = IO::Memory.new if discard_buffer
      @string_pool = StringPool.new if discard_buffer || @string_pool.size > MAX_RETAINED_KEYS
      if discard_buffer
        @token.string_value = ""
        @token.raw_value = ""
      end
    end
  end

  # JSON::Serializable consumes JSON::PullParser specifically. This contained
  # subclass resets the standard parser's connection-local state instead of
  # allocating a lexer, token, key pool, object stack, and string buffer for
  # every frame. reset_state mirrors the private state initialized by the
  # supported Crystal 1.19-1.21 JSON::PullParser implementations.
  private class ReusableJsonPullParser < JSON::PullParser
    def initialize(input : IO)
      super(IO::Memory.new)
      @lexer = ReusableJsonLexer.new(input)
      reset_state
    end

    def reset(input : IO) : self
      @lexer.as(ReusableJsonLexer).reset(input)
      reset_state
    end

    def release_excess(discard_buffer : Bool) : Nil
      @lexer.as(ReusableJsonLexer).release_excess(discard_buffer)
      if discard_buffer
        @string_value = ""
        @raw_value = ""
      end
    end

    private def reset_state : self
      @kind = :EOF
      @bool_value = false
      @string_value = ""
      @raw_value = ""
      @object_stack.clear
      @skip_count = 0
      @location = {0_i64, 0_i64}
      next_token
      read_next
      self
    end
  end
end
