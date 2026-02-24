using NHibernate.Collection;
using NHibernate.Engine;
using NHibernate.Persister.Collection;
using NHibernate.Persister.Entity;
using NHibernate.Proxy;
using NHibernate.Type;

namespace NHibernate.Event.Default
{
	/// <summary> 
	/// When a transient entity is passed to lock(), we must inspect all its collections and
	/// 1. associate any uninitialized PersistentCollections with this session
	/// 2. associate any initialized PersistentCollections with this session, using the existing snapshot
	/// 3. throw an exception for each "new" collection 
	/// </summary>
	public partial class OnLockVisitor : ReattachVisitor
	{
		private readonly bool isLock;
		private bool substitute;

		public OnLockVisitor(IEventSource session, object ownerIdentifier, object owner, bool isLock) : base(session, ownerIdentifier, owner)
		{
			this.isLock = isLock;
		}

		internal object[] SubstituteValues { get; private set; }

		private object ProcessProxy(INHibernateProxy proxy, EntityType entityType)
		{
			var persistenceContext = Session.PersistenceContext;
			var li = proxy.HibernateLazyInitializer;

			IEntityPersister persister = Session.Factory.GetEntityPersister(li.EntityName);
			EntityKey key = Session.GenerateEntityKey(li.Identifier, persister);

			var existingProxy = persistenceContext.GetProxy(key);
			if (existingProxy != null)
			{
				if (existingProxy == proxy)
				{
					return null;
				}

				if (li.IsUninitialized || NHibernateUtil.IsInitialized(existingProxy))
				{
					return existingProxy;
				}

				// if existing proxy is not initialized, we would like to add already loaded implementation. But
				// keep using preexisting proxy (reference equality).
				ProcessLoaded(li.GetImplementation(), entityType, key);
				return existingProxy;
			}
			
			// TODO: should probably check for loaded also
			// if not initialized and not persistent already, then it is from another session
			if (li.IsUninitialized)
			{
				persistenceContext.ReassociateIfUninitializedProxy(proxy);
				return null;
			}

			// if loaded and not from this session, then go to ProcessLoaded to Lock and store.
			// BUT return null to save current proxy (for reference equality)
			var implementation = li.GetImplementation();
			ProcessLoaded(implementation, entityType, key);
			persistenceContext.AddProxy(key, proxy);
			return null;
		}

		private object ProcessLoaded(object loaded, EntityType entityType, EntityKey key)
		{
			var persistenceContext = Session.PersistenceContext;
			var entry = persistenceContext.GetEntry(loaded);

			// build key if loaded wasn't a proxy
			if (key == null)
			{
				var entityPersister = Session.GetEntityPersister(entityType.Name, loaded);
				key = new EntityKey(entityPersister.GetIdentifier(loaded), entityPersister);
			}

			// skip if already in cache
			if (entry != null)
			{
				// substitute with proxy if exists.
				return persistenceContext.GetProxy(key);
			}

			// if proxy exists and is initialized, substitute with proxy.
			var proxy = persistenceContext.GetProxy(key);
			if (proxy != null && NHibernateUtil.IsInitialized(proxy))
			{
				return proxy;
			}

			var entityEntry = persistenceContext.GetEntity(key);
			if (entityEntry != null)
			{
				// can't be 'loaded', guarded by GetEntry call earlier
				return entityEntry;
			}

			// lock entity if loaded and not in cache entityEntry
			Session.Lock(loaded, LockMode.None);

			// substitute with proxy if exists.
			return proxy;
		}

		internal override object ProcessEntity(object value, EntityType entityType)
		{
			if (!isLock || value == null)
			{
				return base.ProcessEntity(value, entityType);
			}

			if (value.IsProxy())
			{
				INHibernateProxy proxy = (INHibernateProxy) value;
				return ProcessProxy(proxy, entityType);
			}
			else
			{
				return ProcessLoaded(value, entityType, null);
			}
		}

		internal override object ProcessCollection(object collection, CollectionType type)
		{
			ISessionImplementor session = Session;
			ICollectionPersister persister = session.Factory.GetCollectionPersister(type.Role);

			if (collection == null)
			{
				//do nothing
			}
			else
			{
				IPersistentCollection persistentCollection = collection as IPersistentCollection;
				if (persistentCollection != null)
				{
					if (persistentCollection.SetCurrentSession(session))
					{
						if (IsOwnerUnchanged(persistentCollection, persister, ExtractCollectionKeyFromOwner(persister)))
						{
							// a "detached" collection that originally belonged to the same entity
							if (persistentCollection.IsDirty)
							{
								throw new HibernateException("reassociated object has dirty collection: " + persistentCollection.Role);
							}

							ReattachCollection(persistentCollection, type);
						}
						else
						{
							// a "detached" collection that belonged to a different entity
							throw new HibernateException("reassociated object has dirty collection reference: " + persistentCollection.Role);
						}
					}
					else
					{
						// a collection loaded in the current session
						// can not possibly be the collection belonging
						// to the entity passed to update()
						throw new HibernateException("reassociated object has dirty collection reference: " + persistentCollection.Role);
					}
				}
				else
				{
					// brand new collection
					//TODO: or an array!! we can't lock objects with arrays now??
					throw new HibernateException("reassociated object has dirty collection reference (or an array)");
				}
			}

			return null;
		}

		internal override void ProcessValue(int i, object[] values, IType[] types)
		{
			object result = ProcessValue(values[i], types[i]);
			if (result != null)
			{
				substitute = true;
				values[i] = result;
			}
		}

		internal override void Process(object obj, IEntityPersister persister)
		{
			object[] values = persister.GetPropertyValues(obj);
			IType[] types = persister.PropertyTypes;
			ProcessEntityPropertyValues(values, types);
			if (substitute)
			{
				persister.SetPropertyValues(obj, values);
				SubstituteValues = values;
			}
		}
	}
}
