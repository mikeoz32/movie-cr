module Movie
  # Akka-style extension id helper for lazy, per-system singletons.
  # T is expected to be a Movie::Extension subclass.
  abstract class ExtensionId(T)
    macro inherited
      @@instance : self?

      def self.instance : self
        @@instance ||= new
      end

      def self.get(system : Movie::AbstractActorSystem) : T
        instance.get(system)
      end
    end

    # Create a new extension instance for the given system.
    abstract def create(system : AbstractActorSystem) : T

    # Returns the extension instance, creating and registering it on first access.
    def get(system : AbstractActorSystem) : T
      if ext = system.extension(T)
        return ext
      end
      ext = create(system)
      system.extensions.get_or_register(T, ext.as(Extension))
    end
  end
end
