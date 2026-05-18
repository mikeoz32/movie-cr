# Remote module for Movie actor system.
# Provides network communication between actor systems.
#
# This file should be required after the base Movie types are defined
# (AbstractActorSystem, ActorRefBase, etc.)

require "./path"
require "./remote/wire_envelope"
require "./remote/message_registry"
require "./remote/frame_codec"
require "./remote/path_registry"
require "./remote/connection"
require "./remote/server"
require "./remote/connection_pool"
require "./remote/remote_actor_ref"
require "./remote/extension"
require "./remote/extension_id"
