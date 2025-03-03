using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NHibernate.Hql.Ast;
using NHibernate.Linq.Expressions;
using NHibernate.Util;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace NHibernate.Linq.Visitors
{
	public class SelectClauseVisitor : RelinqExpressionVisitor
	{
		private readonly HqlTreeBuilder _hqlTreeBuilder = new HqlTreeBuilder();
		private HashSet<Expression> _hqlNodes;
		private readonly ParameterExpression _inputParameter;
		private readonly VisitorParameters _parameters;
		private int _iColumn;
		private List<HqlExpression> _hqlTreeNodes = new List<HqlExpression>();
		private readonly HqlGeneratorExpressionVisitor _hqlVisitor;

		public SelectClauseVisitor(System.Type inputType, VisitorParameters parameters)
		{
			_inputParameter = Expression.Parameter(inputType, "input");
			_parameters = parameters;
			_hqlVisitor = new HqlGeneratorExpressionVisitor(_parameters);
		}

		public LambdaExpression ProjectionExpression { get; private set; }

		public IEnumerable<HqlExpression> GetHqlNodes()
		{
			return _hqlTreeNodes;
		}

		public void VisitSelector(Expression expression) => VisitSelector(expression, false);

		public void VisitSelector(Expression expression, bool isSubQuery)
		{
			var keyExpression = expression;
			var distinct = expression as NhDistinctExpression;
			if (distinct != null)
			{
				expression = distinct.Expression;
			}

			// Find the sub trees that can be expressed purely in HQL
			var nominator = new SelectClauseHqlNominator(_parameters);
			expression = nominator.Nominate(expression, isSubQuery);
			_hqlNodes = nominator.HqlCandidates;

			// Linq2SQL ignores calls to local methods. Linq2EF seems to not support
			// calls to local methods at all. For NHibernate we support local methods,
			// but prevent their use together with server-side distinct, since it may
			// end up being wrong.
			if (distinct != null && nominator.ContainsUntranslatedMethodCalls)
				throw new NotSupportedException("Cannot use distinct on result that depends on methods for which no SQL equivalent exist.");

			// Now visit the tree
			var projection = Visit(expression);

			if (((projection != expression) && !_hqlNodes.Contains(expression)) || projection.NodeType is ExpressionType.New or ExpressionType.Block)
			{
				ProjectionExpression = Expression.Lambda(projection, _inputParameter);
				_parameters.SubQuerySelectToTransformer[keyExpression] = projection;
			}

			// Handle any boolean results in the output nodes
			_hqlTreeNodes = _hqlTreeNodes.ConvertAll(node => node.ToArithmeticExpression());

			if (distinct != null)
			{
				var treeNodes = new List<HqlTreeNode>(_hqlTreeNodes.Count + 1) {_hqlTreeBuilder.Distinct()};
				treeNodes.AddRange(_hqlTreeNodes);
				_hqlTreeNodes = new List<HqlExpression>(1) {_hqlTreeBuilder.ExpressionSubTreeHolder(treeNodes)};
			}
		}

		public override Expression Visit(Expression expression)
		{
			if (expression == null)
			{
				return null;
			}

			bool isHqlNode = _hqlNodes.Contains(expression);
			if (isHqlNode)
			{
				// Pure HQL evaluation
				_hqlTreeNodes.Add(_hqlVisitor.Visit(expression).AsExpression());
			}

			// if node is New node that was translated to AS query
			if (isHqlNode && expression is NewExpression newExpression)
			{
				Expression[] convArgs = new Expression[newExpression.Arguments.Count];

				// I don't know a better way to know if a result is null or just one of it's members is null
				var returnLabel = Expression.Label(newExpression.Type);
				var returnNull = Expression.Return(returnLabel, Expression.Constant(null, newExpression.Type));
				var testers = new List<Expression>(newExpression.Arguments.Count);
				for (int i = 0; i < newExpression.Arguments.Count; i++)
				{
					BinaryExpression binaryExpression = Expression.ArrayIndex(_inputParameter, Expression.Constant(_iColumn++));

					if (!newExpression.Arguments[i].Type.IsNullableOrReference())
					{
						testers.Add(
							Expression.IfThen(Expression.Equal(binaryExpression, Expression.Constant(null)), returnNull)
						);
					}
					
					convArgs[i] = Convert(binaryExpression, newExpression.Arguments[i].Type);
				}

				var ok = Expression.New(newExpression.Constructor, convArgs, newExpression.Members);
				testers.Add(Expression.Label(returnLabel, ok));
				return Expression.Block(testers);
			}
			
			// "New" was translated to HQL in some way, but the result of the query is scalar, without any 'persister'.
			if (isHqlNode && expression.NodeType != ExpressionType.New)
			{
				if (expression is QuerySourceReferenceExpression referenceExpression
				    && _parameters.SubQueryAliasToTransformer.TryGetValue(referenceExpression.ReferencedQuerySource.ItemName, out Expression exp))
				{
					IncrementArrayIndexesVisitor visitor = new(_inputParameter, _iColumn);
					var resExp = visitor.Visit(exp);
					_iColumn += visitor.MaxIndex + 1;
					return resExp;
				}

				return Convert(Expression.ArrayIndex(_inputParameter, Expression.Constant(_iColumn++)), expression.Type);
			}

			// Can't handle this node with HQL.  Just recurse down, and emit the expression
			return base.Visit(expression);
		}

		private static readonly MethodInfo ConvertChangeType =
			ReflectHelper.FastGetMethod(System.Convert.ChangeType, default(object), default(System.Type));

		private static Expression Convert(Expression expression, System.Type type)
		{
			//#1121
			if (type.IsEnum)
			{
				expression = Expression.Call(
					ConvertChangeType,
					expression,
					Expression.Constant(Enum.GetUnderlyingType(type)));
			}

			return Expression.Convert(expression, type);
		}
	}
	
	internal sealed class IncrementArrayIndexesVisitor : RelinqExpressionVisitor
	{
		private readonly ParameterExpression _inputParameter;
		private readonly int _offset;

		public IncrementArrayIndexesVisitor(ParameterExpression inputParameter, int offset)
		{
			_inputParameter = inputParameter;
			_offset = offset;
		}

		public int MaxIndex { get; private set; } = -1;

		protected override Expression VisitParameter(ParameterExpression node)
		{
			return _inputParameter;
		}

		protected override Expression VisitBinary(BinaryExpression node)
		{
			if (node.NodeType != ExpressionType.ArrayIndex || node.Right is not ConstantExpression { Value: int idx })
			{
				return base.VisitBinary(node);
			}
			
			if (idx > MaxIndex)
				MaxIndex = idx;

			Expression left = Visit(node.Left);
			return Expression.ArrayIndex(left, Expression.Constant(
				_offset + idx
			));
		}
	}

	// Since v5
	[Obsolete]
	public static class BooleanToCaseConvertor
	{
		[Obsolete]
		public static IEnumerable<HqlExpression> Convert(IEnumerable<HqlExpression> hqlTreeNodes)
		{
			return hqlTreeNodes.Select(node => node.ToArithmeticExpression());
		}

		[Obsolete]
		public static HqlExpression ConvertBooleanToCase(HqlExpression node)
		{
			return node.ToArithmeticExpression();
		}
	}
}
