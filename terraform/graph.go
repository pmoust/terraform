package terraform

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/hashicorp/terraform/config"
	"github.com/hashicorp/terraform/depgraph"
	"github.com/hashicorp/terraform/helper/multierror"
)

// GraphOpts are options used to create the resource graph that Terraform
// walks to make changes to infrastructure.
//
// Depending on what options are set, the resulting graph will come in
// varying degrees of completeness.
type GraphOpts struct {
	// Config is the configuration from which to build the basic graph.
	// This is the only required item.
	Config *config.Config

	// Diff of changes that will be applied to the given state. This will
	// associate a ResourceDiff with applicable resources. Additionally,
	// new resource nodes representing resource destruction may be inserted
	// into the graph.
	Diff *Diff

	// State, if present, will make the ResourceState available on each
	// resource node. Additionally, any orphans will be added automatically
	// to the graph.
	State *State

	// Providers is a mapping of prefixes to a resource provider. If given,
	// resource providers will be found, initialized, and associated to the
	// resources in the graph.
	//
	// This will also potentially insert new nodes into the graph for
	// the configuration of resource providers.
	Providers map[string]ResourceProviderFactory

	// Provisioners is a mapping of names to a resource provisioner.
	// These must be provided to support resource provisioners.
	Provisioners map[string]ResourceProvisionerFactory
}

// GraphRootNode is the name of the root node in the Terraform resource
// graph. This node is just a placemarker and has no associated functionality.
const GraphRootNode = "root"

// GraphNodeResource is a node type in the graph that represents a resource
// that will be created or managed. Unlike the GraphNodeResourceMeta node,
// this represents a _single_, _resource_ to be managed, not a set of resources
// or a component of a resource.
type GraphNodeResource struct {
	Index              int
	Config             *config.Resource
	Resource           *Resource
	ResourceProviderID string
}

// GraphNodeResourceMeta is a node type in the graph that represents the
// metadata for a resource. There will be one meta node for every resource
// in the configuration.
type GraphNodeResourceMeta struct {
	ID    string
	Name  string
	Type  string
	Count int
}

// GraphNodeResourceProvider is a node type in the graph that represents
// the configuration for a resource provider.
type GraphNodeResourceProvider struct {
	ID           string
	Providers    map[string]ResourceProvider
	ProviderKeys []string
	Config       *config.ProviderConfig
}

// Graph builds a dependency graph of all the resources for infrastructure
// change.
//
// This dependency graph shows the correct order that any resources need
// to be operated on.
//
// The Meta field of a graph Noun can contain one of the follow types. A
// description is next to each type to explain what it is.
//
//   *GraphNodeResource - A resource. See the documentation of this
//     struct for more details.
//   *GraphNodeResourceProvider - A resource provider that needs to be
//     configured at this point.
//
func Graph(opts *GraphOpts) (*depgraph.Graph, error) {
	if opts.Config == nil {
		return nil, errors.New("Config is required for Graph")
	}

	log.Printf("[DEBUG] Creating graph...")

	g := new(depgraph.Graph)

	// First, build the initial resource graph. This only has the resources
	// and no dependencies. This only adds resources that are in the config
	// and not "orphans" (that are in the state, but not in the config).
	graphAddConfigResources(g, opts.Config, opts.State)

	// Add explicit dependsOn dependencies to the graph
	graphAddExplicitDeps(g)

	if opts.State != nil {
		// Next, add the state orphans if we have any
		graphAddOrphans(g, opts.Config, opts.State)

		// Add tainted resources if we have any.
		graphAddTainted(g, opts.State)
	}

	// Map the provider configurations to all of the resources
	graphAddProviderConfigs(g, opts.Config)

	// Setup the provisioners. These may have variable dependencies,
	// and must be done before dependency setup
	if err := graphMapResourceProvisioners(g, opts.Provisioners); err != nil {
		return nil, err
	}

	// Add all the variable dependencies
	graphAddVariableDeps(g)

	// Build the root so that we have a single valid root
	graphAddRoot(g)

	// If providers were given, lets associate the proper providers and
	// instantiate them.
	if len(opts.Providers) > 0 {
		// Add missing providers from the mapping
		if err := graphAddMissingResourceProviders(g, opts.Providers); err != nil {
			return nil, err
		}

		// Initialize all the providers
		if err := graphInitResourceProviders(g, opts.Providers); err != nil {
			return nil, err
		}

		// Map the providers to resources
		if err := graphMapResourceProviders(g); err != nil {
			return nil, err
		}
	}

	// If we have a diff, then make sure to add that in
	if opts.Diff != nil {
		if err := graphAddDiff(g, opts.Diff); err != nil {
			return nil, err
		}
	}

	// Validate
	if err := g.Validate(); err != nil {
		return nil, err
	}

	log.Printf(
		"[DEBUG] Graph created and valid. %d nouns.",
		len(g.Nouns))

	return g, nil
}

// graphInitState is used to initialize a State with a ResourceState
// for every resource.
//
// This method is very important to call because it will properly setup
// the ResourceState dependency information with data from the graph. This
// allows orphaned resources to be destroyed in the proper order.
func graphInitState(s *State, g *depgraph.Graph) {
	// TODO: other modules
	mod := s.RootModule()

	for _, n := range g.Nouns {
		// Ignore any non-resource nodes
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}
		r := rn.Resource
		rs := mod.Resources[r.Id]
		if rs == nil {
			rs = new(ResourceState)
			rs.init()
			mod.Resources[r.Id] = rs
		}

		// Update the dependencies
		var inject []string
		for _, dep := range n.Deps {
			switch target := dep.Target.Meta.(type) {
			case *GraphNodeResource:
				if target.Resource.Id == r.Id {
					continue
				}
				inject = append(inject, target.Resource.Id)

			case *GraphNodeResourceMeta:
				// Inject each sub-resource as a depedency
				for i := 0; i < target.Count; i++ {
					id := fmt.Sprintf("%s.%d", target.ID, i)
					inject = append(inject, id)
				}
			}
		}

		// Update the dependencies
		rs.Dependencies = inject
	}
}

// configGraph turns a configuration structure into a dependency graph.
func graphAddConfigResources(
	g *depgraph.Graph, c *config.Config, s *State) {
	// TODO: Handle non-root modules
	mod := s.ModuleByPath(rootModulePath)

	// This tracks all the resource nouns
	nouns := make(map[string]*depgraph.Noun)
	for _, r := range c.Resources {
		resourceNouns := make([]*depgraph.Noun, r.Count)
		for i := 0; i < r.Count; i++ {
			name := r.Id()
			index := -1

			// If we have a count that is more than one, then make sure
			// we suffix with the number of the resource that this is.
			if r.Count > 1 {
				name = fmt.Sprintf("%s.%d", name, i)
				index = i
			}

			var state *ResourceState
			if s != nil && mod != nil {
				// Lookup the resource state
				state = mod.Resources[name]
				if state == nil {
					if r.Count == 1 {
						// If the count is one, check the state for ".0"
						// appended, which might exist if we go from
						// count > 1 to count == 1.
						state = mod.Resources[r.Id()+".0"]
					} else if i == 0 {
						// If count is greater than one, check for state
						// with just the ID, which might exist if we go
						// from count == 1 to count > 1
						state = mod.Resources[r.Id()]
					}
				}
			}

			if state == nil {
				state = &ResourceState{
					Type: r.Type,
				}
			}

			flags := FlagPrimary
			if len(state.Tainted) > 0 {
				flags |= FlagHasTainted
			}

			resourceNouns[i] = &depgraph.Noun{
				Name: name,
				Meta: &GraphNodeResource{
					Index:  index,
					Config: r,
					Resource: &Resource{
						Id:     name,
						Info:   &InstanceInfo{Type: r.Type},
						State:  state.Primary,
						Config: NewResourceConfig(r.RawConfig),
						Flags:  flags,
					},
				},
			}
		}

		// If we have more than one, then create a meta node to track
		// the resources.
		if r.Count > 1 {
			metaNoun := &depgraph.Noun{
				Name: r.Id(),
				Meta: &GraphNodeResourceMeta{
					ID:    r.Id(),
					Name:  r.Name,
					Type:  r.Type,
					Count: r.Count,
				},
			}

			// Create the dependencies on this noun
			for _, n := range resourceNouns {
				metaNoun.Deps = append(metaNoun.Deps, &depgraph.Dependency{
					Name:   n.Name,
					Source: metaNoun,
					Target: n,
				})
			}

			// Assign it to the map so that we have it
			nouns[metaNoun.Name] = metaNoun
		}

		for _, n := range resourceNouns {
			nouns[n.Name] = n
		}
	}

	// Build the list of nouns that we iterate over
	nounsList := make([]*depgraph.Noun, 0, len(nouns))
	for _, n := range nouns {
		nounsList = append(nounsList, n)
	}

	g.Name = "terraform"
	g.Nouns = append(g.Nouns, nounsList...)
}

// graphAddDiff takes an already-built graph of resources and adds the
// diffs to the resource nodes themselves.
//
// This may also introduces new graph elements. If there are diffs that
// require a destroy, new elements may be introduced since destroy order
// is different than create order. For example, destroying a VPC requires
// destroying the VPC's subnets first, whereas creating a VPC requires
// doing it before the subnets are created. This function handles inserting
// these nodes for you.
func graphAddDiff(g *depgraph.Graph, d *Diff) error {
	var nlist []*depgraph.Noun
	injected := make(map[*depgraph.Dependency]struct{})
	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}
		if rn.Resource.Flags&FlagTainted != 0 {
			continue
		}

		rd, ok := d.Resources[rn.Resource.Id]
		if !ok {
			continue
		}
		if rd.Empty() {
			continue
		}

		if rd.Destroy {
			// If we're destroying, we create a new destroy node with
			// the proper dependencies. Perform a dirty copy operation.
			newNode := new(GraphNodeResource)
			*newNode = *rn
			newNode.Resource = new(Resource)
			*newNode.Resource = *rn.Resource

			// Make the diff _just_ the destroy.
			newNode.Resource.Diff = &InstanceDiff{Destroy: true}

			// Create the new node
			newN := &depgraph.Noun{
				Name: fmt.Sprintf("%s (destroy)", newNode.Resource.Id),
				Meta: newNode,
			}
			newN.Deps = make([]*depgraph.Dependency, len(n.Deps))

			// Copy all the dependencies and do a fixup later
			copy(newN.Deps, n.Deps)

			// Append it to the list so we handle it later
			nlist = append(nlist, newN)

			// Mark the old diff to not destroy since we handle that in
			// the dedicated node.
			newDiff := new(InstanceDiff)
			*newDiff = *rd
			newDiff.Destroy = false
			rd = newDiff

			// The dependency ordering depends on if the CreateBeforeDestroy
			// flag is enabled. If so, we must create the replacement first,
			// and then destroy the old instance.
			if rn.Config != nil && rn.Config.Lifecycle.CreateBeforeDestroy && !rd.Empty() {
				dep := &depgraph.Dependency{
					Name:   n.Name,
					Source: newN,
					Target: n,
				}

				// Add the old noun to the new noun dependencies so that
				// the create happens before the destroy.
				newN.Deps = append(newN.Deps, dep)

				// Mark that this dependency has been injected so that
				// we do not invert the direction below.
				injected[dep] = struct{}{}

				// Add a depedency from the root, since the create node
				// does not depend on us
				g.Root.Deps = append(g.Root.Deps, &depgraph.Dependency{
					Name:   newN.Name,
					Source: g.Root,
					Target: newN,
				})

			} else {
				dep := &depgraph.Dependency{
					Name:   newN.Name,
					Source: n,
					Target: newN,
				}

				// Add the new noun to our dependencies so that
				// the destroy happens before the apply.
				n.Deps = append(n.Deps, dep)
			}
		}

		rn.Resource.Diff = rd
	}

	// Go through each noun and make sure we calculate all the dependencies
	// properly.
	for _, n := range nlist {
		deps := n.Deps
		num := len(deps)
		for i := 0; i < num; i++ {
			dep := deps[i]

			// Check if this dependency was just injected, otherwise
			// we will incorrectly flip the depedency twice.
			if _, ok := injected[dep]; ok {
				continue
			}

			switch target := dep.Target.Meta.(type) {
			case *GraphNodeResource:
				// If the other node is also being deleted,
				// we must be deleted first. E.g. if A -> B,
				// then when we create, B is created first then A.
				// On teardown, A is destroyed first, then B.
				// Thus we must flip our depedency and instead inject
				// it on B.
				for _, n2 := range nlist {
					rn2 := n2.Meta.(*GraphNodeResource)
					if target.Resource.Id == rn2.Resource.Id {
						newDep := &depgraph.Dependency{
							Name:   n.Name,
							Source: n2,
							Target: n,
						}
						injected[newDep] = struct{}{}
						n2.Deps = append(n2.Deps, newDep)
						break
					}
				}

				// Drop the dependency. We may have created
				// an inverse depedency if the dependent resource
				// is also being deleted, but this dependence is
				// no longer required.
				deps[i], deps[num-1] = deps[num-1], nil
				num--
				i--

			case *GraphNodeResourceMeta:
				// Check if any of the resources part of the meta node
				// are being destroyed, because we must be destroyed first.
				for i := 0; i < target.Count; i++ {
					id := fmt.Sprintf("%s.%d", target.ID, i)
					for _, n2 := range nlist {
						rn2 := n2.Meta.(*GraphNodeResource)
						if id == rn2.Resource.Id {
							newDep := &depgraph.Dependency{
								Name:   n.Name,
								Source: n2,
								Target: n,
							}
							injected[newDep] = struct{}{}
							n2.Deps = append(n2.Deps, newDep)
							break
						}
					}
				}

				// Drop the dependency, since there is
				// nothing that needs to be done for a meta
				// resource on destroy.
				deps[i], deps[num-1] = deps[num-1], nil
				num--
				i--

			case *GraphNodeResourceProvider:
				// Keep these around, but fix up the source to be ourselves
				// rather than the old node.
				newDep := *dep
				newDep.Source = n
				deps[i] = &newDep
			default:
				panic(fmt.Errorf("Unhandled depedency type: %#v", dep.Meta))
			}
		}
		n.Deps = deps[:num]
	}

	// Add the nouns to the graph
	g.Nouns = append(g.Nouns, nlist...)

	return nil
}

// graphAddExplicitDeps adds the dependencies to the graph for the explicit
// dependsOn configurations.
func graphAddExplicitDeps(g *depgraph.Graph) {
	depends := false

	rs := make(map[string]*depgraph.Noun)
	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}

		rs[rn.Resource.Id] = n
		if len(rn.Config.DependsOn) > 0 {
			depends = true
		}
	}

	// If we didn't have any dependsOn, just return
	if !depends {
		return
	}

	for _, n1 := range rs {
		rn1 := n1.Meta.(*GraphNodeResource)
		for _, d := range rn1.Config.DependsOn {
			for _, n2 := range rs {
				rn2 := n2.Meta.(*GraphNodeResource)
				if rn2.Config.Id() != d {
					continue
				}

				n1.Deps = append(n1.Deps, &depgraph.Dependency{
					Name:   d,
					Source: n1,
					Target: n2,
				})
			}
		}
	}
}

// graphAddMissingResourceProviders adds GraphNodeResourceProvider nodes for
// the resources that do not have an explicit resource provider specified
// because no provider configuration was given.
func graphAddMissingResourceProviders(
	g *depgraph.Graph,
	ps map[string]ResourceProviderFactory) error {
	var errs []error

	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}
		if rn.ResourceProviderID != "" {
			continue
		}

		prefixes := matchingPrefixes(rn.Resource.Info.Type, ps)
		if len(prefixes) == 0 {
			errs = append(errs, fmt.Errorf(
				"No matching provider for type: %s",
				rn.Resource.Info.Type))
			continue
		}

		// The resource provider ID is simply the shortest matching
		// prefix, since that'll give us the most resource providers
		// to choose from.
		rn.ResourceProviderID = prefixes[len(prefixes)-1]

		// If we don't have a matching noun for this yet, insert it.
		pn := g.Noun(fmt.Sprintf("provider.%s", rn.ResourceProviderID))
		if pn == nil {
			pn = &depgraph.Noun{
				Name: fmt.Sprintf("provider.%s", rn.ResourceProviderID),
				Meta: &GraphNodeResourceProvider{
					ID:     rn.ResourceProviderID,
					Config: nil,
				},
			}
			g.Nouns = append(g.Nouns, pn)
		}

		// Add the provider configuration noun as a dependency
		dep := &depgraph.Dependency{
			Name:   pn.Name,
			Source: n,
			Target: pn,
		}
		n.Deps = append(n.Deps, dep)
	}

	if len(errs) > 0 {
		return &multierror.Error{Errors: errs}
	}

	return nil
}

// graphAddOrphans adds the orphans to the graph.
func graphAddOrphans(g *depgraph.Graph, c *config.Config, s *State) {
	// TODO: Handle other modules
	mod := s.ModuleByPath(rootModulePath)
	if mod == nil {
		return
	}
	var nlist []*depgraph.Noun
	for _, k := range mod.Orphans(c) {
		rs := mod.Resources[k]
		noun := &depgraph.Noun{
			Name: k,
			Meta: &GraphNodeResource{
				Index: -1,
				Resource: &Resource{
					Id:     k,
					Info:   &InstanceInfo{Type: rs.Type},
					State:  rs.Primary,
					Config: NewResourceConfig(nil),
					Flags:  FlagOrphan,
				},
			},
		}

		// Append it to the list so we handle it later
		nlist = append(nlist, noun)
	}

	// Add the nouns to the graph
	g.Nouns = append(g.Nouns, nlist...)

	// Handle the orphan dependencies after adding them
	// to the graph because there may be depedencies between the
	// orphans that otherwise cannot be handled
	for _, n := range nlist {
		rn := n.Meta.(*GraphNodeResource)

		// If we have no dependencies, then just continue
		rs := mod.Resources[n.Name]
		if len(rs.Dependencies) == 0 {
			continue
		}

		for _, n2 := range g.Nouns {
			rn2, ok := n2.Meta.(*GraphNodeResource)
			if !ok {
				continue
			}

			// Don't ever depend on ourselves
			if rn2 == rn {
				continue
			}

			for _, depName := range rs.Dependencies {
				if rn2.Resource.Id != depName {
					continue
				}
				dep := &depgraph.Dependency{
					Name:   depName,
					Source: n,
					Target: n2,
				}
				n.Deps = append(n.Deps, dep)
			}
		}
	}
}

// graphAddProviderConfigs cycles through all the resource-like nodes
// and adds the provider configuration nouns into the tree.
func graphAddProviderConfigs(g *depgraph.Graph, c *config.Config) {
	nounsList := make([]*depgraph.Noun, 0, 2)
	pcNouns := make(map[string]*depgraph.Noun)
	for _, noun := range g.Nouns {
		resourceNode, ok := noun.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}

		// Look up the provider config for this resource
		pcName := config.ProviderConfigName(
			resourceNode.Resource.Info.Type, c.ProviderConfigs)
		if pcName == "" {
			continue
		}

		// We have one, so build the noun if it hasn't already been made
		pcNoun, ok := pcNouns[pcName]
		if !ok {
			var pc *config.ProviderConfig
			for _, v := range c.ProviderConfigs {
				if v.Name == pcName {
					pc = v
					break
				}
			}
			if pc == nil {
				panic("pc not found")
			}

			pcNoun = &depgraph.Noun{
				Name: fmt.Sprintf("provider.%s", pcName),
				Meta: &GraphNodeResourceProvider{
					ID:     pcName,
					Config: pc,
				},
			}
			pcNouns[pcName] = pcNoun
			nounsList = append(nounsList, pcNoun)
		}

		// Set the resource provider ID for this noun so we can look it
		// up later easily.
		resourceNode.ResourceProviderID = pcName

		// Add the provider configuration noun as a dependency
		dep := &depgraph.Dependency{
			Name:   pcName,
			Source: noun,
			Target: pcNoun,
		}
		noun.Deps = append(noun.Deps, dep)
	}

	// Add all the provider config nouns to the graph
	g.Nouns = append(g.Nouns, nounsList...)
}

// graphAddRoot adds a root element to the graph so that there is a single
// root to point to all the dependencies.
func graphAddRoot(g *depgraph.Graph) {
	root := &depgraph.Noun{Name: GraphRootNode}
	for _, n := range g.Nouns {
		switch m := n.Meta.(type) {
		case *GraphNodeResource:
			// If the resource is part of a group, we don't need to make a dep
			if m.Index != -1 {
				continue
			}
		case *GraphNodeResourceMeta:
			// Always in the graph
		case *GraphNodeResourceProvider:
			// ResourceProviders don't need to be in the root deps because
			// they're always pointed to by some resource.
			continue
		}

		root.Deps = append(root.Deps, &depgraph.Dependency{
			Name:   n.Name,
			Source: root,
			Target: n,
		})
	}
	g.Nouns = append(g.Nouns, root)
	g.Root = root
}

// graphAddVariableDeps inspects all the nouns and adds any dependencies
// based on variable values.
func graphAddVariableDeps(g *depgraph.Graph) {
	for _, n := range g.Nouns {
		var vars map[string]config.InterpolatedVariable
		switch m := n.Meta.(type) {
		case *GraphNodeResource:
			if m.Config != nil {
				// Handle the resource variables
				vars = m.Config.RawConfig.Variables
				nounAddVariableDeps(g, n, vars, false)
			}

			// Handle the variables of the resource provisioners
			for _, p := range m.Resource.Provisioners {
				vars = p.RawConfig.Variables
				nounAddVariableDeps(g, n, vars, true)

				vars = p.ConnInfo.Variables
				nounAddVariableDeps(g, n, vars, true)
			}

		case *GraphNodeResourceProvider:
			vars = m.Config.RawConfig.Variables
			nounAddVariableDeps(g, n, vars, false)

		default:
			continue
		}
	}
}

// graphAddTainted adds the tainted instances to the graph.
func graphAddTainted(g *depgraph.Graph, s *State) {
	// TODO: Handle other modules
	mod := s.ModuleByPath(rootModulePath)
	if mod == nil {
		return
	}

	var nlist []*depgraph.Noun
	for k, rs := range mod.Resources {
		// If we have no tainted resources, continue on
		if len(rs.Tainted) == 0 {
			continue
		}

		// Find the untainted resource of this in the noun list
		var untainted *depgraph.Noun
		for _, n := range g.Nouns {
			if n.Name == k {
				untainted = n
				break
			}
		}

		for i, is := range rs.Tainted {
			name := fmt.Sprintf("%s (tainted #%d)", k, i+1)

			// Add each of the tainted resources to the graph, and encode
			// a dependency from the non-tainted resource to this so that
			// tainted resources are always destroyed first.
			noun := &depgraph.Noun{
				Name: name,
				Meta: &GraphNodeResource{
					Index: -1,
					Resource: &Resource{
						Id:           k,
						Info:         &InstanceInfo{Type: rs.Type},
						State:        is,
						Config:       NewResourceConfig(nil),
						Diff:         &InstanceDiff{Destroy: true},
						Flags:        FlagTainted,
						TaintedIndex: i,
					},
				},
			}

			// Append it to the list so we handle it later
			nlist = append(nlist, noun)

			// If we have an untainted version, then make sure to add
			// the dependency.
			if untainted != nil {
				dep := &depgraph.Dependency{
					Name:   name,
					Source: untainted,
					Target: noun,
				}

				untainted.Deps = append(untainted.Deps, dep)
			}
		}
	}

	// Add the nouns to the graph
	g.Nouns = append(g.Nouns, nlist...)
}

// nounAddVariableDeps updates the dependencies of a noun given
// a set of associated variable values
func nounAddVariableDeps(
	g *depgraph.Graph,
	n *depgraph.Noun,
	vars map[string]config.InterpolatedVariable,
	removeSelf bool) {
	for _, v := range vars {
		// Only resource variables impose dependencies
		rv, ok := v.(*config.ResourceVariable)
		if !ok {
			continue
		}

		// Find the target
		target := g.Noun(rv.ResourceId())
		if target == nil {
			continue
		}

		// If we're ignoring self-references, then don't add that
		// dependency.
		if removeSelf && n == target {
			continue
		}

		// Build the dependency
		dep := &depgraph.Dependency{
			Name:   rv.ResourceId(),
			Source: n,
			Target: target,
		}

		n.Deps = append(n.Deps, dep)
	}
}

// graphInitResourceProviders maps the resource providers onto the graph
// given a mapping of prefixes to resource providers.
//
// Unlike the graphAdd* functions, this one can return an error if resource
// providers can't be found or can't be instantiated.
func graphInitResourceProviders(
	g *depgraph.Graph,
	ps map[string]ResourceProviderFactory) error {
	var errs []error

	// Keep track of providers we know we couldn't instantiate so
	// that we don't get a ton of errors about the same provider.
	failures := make(map[string]struct{})

	for _, n := range g.Nouns {
		// We only care about the resource providers first. There is guaranteed
		// to be only one node per tuple (providerId, providerConfig), which
		// means we don't need to verify we have instantiated it before.
		rn, ok := n.Meta.(*GraphNodeResourceProvider)
		if !ok {
			continue
		}

		prefixes := matchingPrefixes(rn.ID, ps)
		if len(prefixes) > 0 {
			if _, ok := failures[prefixes[0]]; ok {
				// We already failed this provider, meaning this
				// resource will never succeed, so just continue.
				continue
			}
		}

		// Go through each prefix and instantiate if necessary, then
		// verify if this provider is of use to us or not.
		rn.Providers = make(map[string]ResourceProvider)
		rn.ProviderKeys = prefixes
		for _, prefix := range prefixes {
			p, err := ps[prefix]()
			if err != nil {
				errs = append(errs, fmt.Errorf(
					"Error instantiating resource provider for "+
						"prefix %s: %s", prefix, err))

				// Record the error so that we don't check it again
				failures[prefix] = struct{}{}

				// Jump to the next prefix
				continue
			}

			rn.Providers[prefix] = p
		}

		// If we never found a provider, then error and continue
		if len(rn.Providers) == 0 {
			errs = append(errs, fmt.Errorf(
				"Provider for configuration '%s' not found.",
				rn.ID))
			continue
		}
	}

	if len(errs) > 0 {
		return &multierror.Error{Errors: errs}
	}

	return nil
}

// graphMapResourceProviders takes a graph that already has initialized
// the resource providers (using graphInitResourceProviders) and maps the
// resource providers to the resources themselves.
func graphMapResourceProviders(g *depgraph.Graph) error {
	var errs []error

	// First build a mapping of resource provider ID to the node that
	// contains those resources.
	mapping := make(map[string]*GraphNodeResourceProvider)
	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResourceProvider)
		if !ok {
			continue
		}
		mapping[rn.ID] = rn
	}

	// Now go through each of the resources and find a matching provider.
	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}

		rpn, ok := mapping[rn.ResourceProviderID]
		if !ok {
			// This should never happen since when building the graph
			// we ensure that everything matches up.
			panic(fmt.Sprintf(
				"Resource provider ID not found: %s (type: %s)",
				rn.ResourceProviderID,
				rn.Resource.Info.Type))
		}

		var provider ResourceProvider
		for _, k := range rpn.ProviderKeys {
			// Only try this provider if it has the right prefix
			if !strings.HasPrefix(rn.Resource.Info.Type, k) {
				continue
			}

			rp := rpn.Providers[k]
			if ProviderSatisfies(rp, rn.Resource.Info.Type) {
				provider = rp
				break
			}
		}

		if provider == nil {
			errs = append(errs, fmt.Errorf(
				"Resource provider not found for resource type '%s'",
				rn.Resource.Info.Type))
			continue
		}

		rn.Resource.Provider = provider
	}

	if len(errs) > 0 {
		return &multierror.Error{Errors: errs}
	}

	return nil
}

// graphMapResourceProvisioners takes a graph that already has
// the resources and maps the resource provisioners to the resources themselves.
func graphMapResourceProvisioners(g *depgraph.Graph,
	provisioners map[string]ResourceProvisionerFactory) error {
	var errs []error

	// Create a cache of resource provisioners, avoids duplicate
	// initialization of the instances
	cache := make(map[string]ResourceProvisioner)

	// Go through each of the resources and find a matching provisioners
	for _, n := range g.Nouns {
		rn, ok := n.Meta.(*GraphNodeResource)
		if !ok {
			continue
		}

		// Ignore orphan nodes with no provisioners
		if rn.Config == nil {
			continue
		}

		// Check each provisioner
		for _, p := range rn.Config.Provisioners {
			// Check for a cached provisioner
			provisioner, ok := cache[p.Type]
			if !ok {
				// Lookup the factory method
				factory, ok := provisioners[p.Type]
				if !ok {
					errs = append(errs, fmt.Errorf(
						"Resource provisioner not found for provisioner type '%s'",
						p.Type))
					continue
				}

				// Initialize the provisioner
				prov, err := factory()
				if err != nil {
					errs = append(errs, fmt.Errorf(
						"Failed to instantiate provisioner type '%s': %v",
						p.Type, err))
					continue
				}
				provisioner = prov

				// Cache this type of provisioner
				cache[p.Type] = prov
			}

			// Save the provisioner
			rn.Resource.Provisioners = append(rn.Resource.Provisioners, &ResourceProvisionerConfig{
				Type:        p.Type,
				Provisioner: provisioner,
				Config:      NewResourceConfig(p.RawConfig),
				RawConfig:   p.RawConfig,
				ConnInfo:    p.ConnInfo,
			})
		}
	}

	if len(errs) > 0 {
		return &multierror.Error{Errors: errs}
	}
	return nil
}

// matchingPrefixes takes a resource type and a set of resource
// providers we know about by prefix and returns a list of prefixes
// that might be valid for that resource.
//
// The list returned is in the order that they should be attempted.
func matchingPrefixes(
	t string,
	ps map[string]ResourceProviderFactory) []string {
	result := make([]string, 0, 1)
	for prefix, _ := range ps {
		if strings.HasPrefix(t, prefix) {
			result = append(result, prefix)
		}
	}

	// Sort by longest first
	sort.Sort(stringLenSort(result))

	return result
}

// stringLenSort implements sort.Interface and sorts strings in increasing
// length order. i.e. "a", "aa", "aaa"
type stringLenSort []string

func (s stringLenSort) Len() int {
	return len(s)
}

func (s stringLenSort) Less(i, j int) bool {
	return len(s[i]) < len(s[j])
}

func (s stringLenSort) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
