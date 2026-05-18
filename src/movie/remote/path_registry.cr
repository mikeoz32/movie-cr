require "../path"

module Movie::Remote
  # PathRegistry in Remote module is deprecated - use Movie::PathRegistry instead.
  # This file is kept for backward compatibility but delegates to the core PathRegistry.
  # The core PathRegistry uses normalized keys to match paths regardless of protocol/host/port.
end
