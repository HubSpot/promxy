package parsehelper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
"github.com/prometheus/prometheus/promql"
	"github.com/sirupsen/logrus"
)

type inspector func(promql.Node, []promql.Node) error

func (f inspector) Visit(node promql.Node, path []promql.Node) (promql.Visitor, error) {
	if err := f(node, path); err != nil {
		return nil, err
	}

	return f, nil
}

func ExtractSelectors(query string) ([][]*labels.Matcher, error) {
	expr, err := promql.ParseExpr(query)

	if err != nil {
		return nil, err
	}

	var selectors [][]*labels.Matcher
	inspect(expr, func(node promql.Node, _ []promql.Node) error {
		vs, ok := node.(*promql.VectorSelector)
		if ok {
			selectors = append(selectors, vs.LabelMatchers)
		}
		return nil
	})
	return selectors, nil
}

func inspect(node promql.Node, f inspector) {
	//nolint: errcheck
	walk(inspector(f), node, nil)
}

func walk(v promql.Visitor, node promql.Node, path []promql.Node) error {
	var err error
	if v, err = v.Visit(node, path); v == nil || err != nil {
		return err
	}
	path = append(path, node)


	for _, e := range children(node) {
		if err := walk(v, e, path); err != nil {
			return err
		}
	}

	_, err = v.Visit(nil, nil)
	return err
}

// Children returns a list of all child nodes of a syntax tree node.
func children(node promql.Node) []promql.Node {
	// For some reasons these switches have significantly better performance than interfaces
	switch n := node.(type) {
	case *promql.EvalStmt:
		return []promql.Node{n.Expr}
	case promql.Expressions:
		// golang cannot convert slices of interfaces
		ret := make([]promql.Node, len(n))
		for i, e := range n {
			ret[i] = e
		}
		return ret
	case *promql.AggregateExpr:
		// While this does not look nice, it should avoid unnecessary allocations
		// caused by slice resizing
		if n.Expr == nil && n.Param == nil {
			return nil
		} else if n.Expr == nil {
			return []promql.Node{n.Param}
		} else if n.Param == nil {
			return []promql.Node{n.Expr}
		} else {
			return []promql.Node{n.Expr, n.Param}
		}
	case *promql.BinaryExpr:
		return []promql.Node{n.LHS, n.RHS}
	case *promql.Call:
		// golang cannot convert slices of interfaces
		ret := make([]promql.Node, len(n.Args))
		for i, e := range n.Args {
			ret[i] = e
		}
		return ret
	case *promql.SubqueryExpr:
		return []promql.Node{n.Expr}
	case *promql.ParenExpr:
		return []promql.Node{n.Expr}
	case *promql.UnaryExpr:
		return []promql.Node{n.Expr}
	case *promql.MatrixSelector:
		vs := promql.VectorSelector{
			Name:          n.Name,
			Offset:        n.Offset,
			LabelMatchers: n.LabelMatchers,
			LookbackDelta: 1000,
		}
		
		return []promql.Node{&vs}
	case *promql.NumberLiteral, *promql.StringLiteral, *promql.VectorSelector:
		// nothing to do
		return []promql.Node{}
	default:
		logrus.Error("promql.Children: unhandled node type %T", node)
		panic(errors.Errorf("promql.Children: unhandled node type %T", node))
	}
}
