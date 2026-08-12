package sequence

const (
	// VersionMetaKey carries the client route version on FetchNext requests.
	VersionMetaKey = "routerVersion"
	// NodeIDAttribute identifies a discovered endpoint in a route snapshot.
	NodeIDAttribute = "node_id"
	// BalancerType is the registered Yggdrasil sequence balancer type.
	BalancerType = "sequence"
	// InterceptorName is the registered Yggdrasil sequence client interceptor.
	InterceptorName = "sequence"
	// ModuleName is the stable Yggdrasil routing module name.
	ModuleName = "skuld.sequence.routing"
)
