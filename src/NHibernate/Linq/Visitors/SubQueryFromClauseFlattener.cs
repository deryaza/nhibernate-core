using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionVisitors;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.EagerFetching;

namespace NHibernate.Linq.Visitors
{
	public class SubQueryFromClauseFlattener : NhQueryModelVisitorBase
	{
		private static readonly System.Type[] FlattenableResultOperators =
		{
			typeof(LockResultOperator),
			typeof(FetchLazyPropertiesResultOperator),
			typeof(FetchOneRequest),
			typeof(FetchManyRequest),
			typeof(AsQueryableResultOperator)
		};

		public static void ReWrite(QueryModel queryModel)
		{
			new SubQueryFromClauseFlattener().VisitQueryModel(queryModel);
		}

		public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index)
		{
			if (fromClause.FromExpression is SubQueryExpression subQueryExpression)
				FlattenSubQuery(subQueryExpression, fromClause, queryModel, index + 1);
			base.VisitAdditionalFromClause(fromClause, queryModel, index);
		}

		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
		{
			if (fromClause.FromExpression is SubQueryExpression subQueryExpression)
				FlattenSubQuery(subQueryExpression, fromClause, queryModel, 0);
			base.VisitMainFromClause(fromClause, queryModel);
		}

		private static bool CheckFlattenable(QueryModel subQueryModel)
		{
			if (subQueryModel.BodyClauses.OfType<OrderByClause>().Any()) 
				return false;

			if (subQueryModel.SelectClause.Selector.NodeType is ExpressionType.MemberInit or ExpressionType.New)
				return false;

			if (subQueryModel.ResultOperators.Count == 0) 
				return true;
			
			return HasJustAllFlattenableOperator(subQueryModel.ResultOperators);
		}

		private static bool HasJustAllFlattenableOperator(IEnumerable<ResultOperatorBase> resultOperators)
		{
			return resultOperators.All(x => FlattenableResultOperators.Contains(x.GetType()));
		}

		private static void CopyFromClauseData(FromClauseBase source, FromClauseBase destination)
		{
			destination.FromExpression = source.FromExpression;
			destination.ItemName = source.ItemName;
			destination.ItemType = source.ItemType;
		}

		private static void FlattenSubQuery(SubQueryExpression subQueryExpression, FromClauseBase fromClause, QueryModel queryModel, int destinationIndex)
		{
			if (!CheckFlattenable(subQueryExpression.QueryModel))
				return;

			var mainFromClause = subQueryExpression.QueryModel.MainFromClause;
			CopyFromClauseData(mainFromClause, fromClause);

			var innerSelectorMapping = new QuerySourceMapping();
			innerSelectorMapping.AddMapping(fromClause, subQueryExpression.QueryModel.SelectClause.Selector);
			queryModel.TransformExpressions(ex => ReferenceReplacingExpressionVisitor.ReplaceClauseReferences(ex, innerSelectorMapping, false));

			InsertBodyClauses(subQueryExpression.QueryModel.BodyClauses, queryModel, destinationIndex);
			InsertResultOperators(subQueryExpression.QueryModel.ResultOperators, queryModel);

			var innerBodyClauseMapping = new QuerySourceMapping();
			innerBodyClauseMapping.AddMapping(mainFromClause, new QuerySourceReferenceExpression(fromClause));
			queryModel.TransformExpressions(ex => ReferenceReplacingExpressionVisitor.ReplaceClauseReferences(ex, innerBodyClauseMapping, false));
		}

		internal static void InsertResultOperators(IEnumerable<ResultOperatorBase> resultOperators, QueryModel queryModel)
		{
			var index = 0;
			foreach (var bodyClause in resultOperators)
			{
				queryModel.ResultOperators.Insert(index, bodyClause);
				++index;
			}
		}

		private static void InsertBodyClauses(IEnumerable<IBodyClause> bodyClauses, QueryModel queryModel, int destinationIndex)
		{
			foreach (var bodyClause in  bodyClauses)
			{
				queryModel.BodyClauses.Insert(destinationIndex, bodyClause);
				++destinationIndex;
			}
		}
	}
}
