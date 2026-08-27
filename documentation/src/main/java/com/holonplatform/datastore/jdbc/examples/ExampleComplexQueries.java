/*
 * Copyright 2016-2017 Axioma srl.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.holonplatform.datastore.jdbc.examples;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Examples of complex SQL queries using Java Text Blocks (JEP 378 - Java 15+).
 * 
 * Text blocks provide improved readability for multi-line SQL strings by:
 * - Eliminating string concatenation and escape characters
 * - Preserving formatting and indentation
 * - Reducing boilerplate and improving maintainability
 * 
 * @since 11.1.0
 */
@SuppressWarnings("unused")
public class ExampleComplexQueries {

	// tag::text-blocks-intro[]
	/**
	 * Text Blocks example: Readable multi-line SQL with proper formatting
	 */
	public void complexSelectQuery(Connection connection) throws Exception {
		// Using Text Blocks (Java 15+) - Clean, readable, no escaping needed
		String query = """
				SELECT u.id, u.username, u.email,
				       COUNT(p.id) as post_count,
				       MAX(p.created_at) as last_post_date
				FROM users u
				LEFT JOIN posts p ON u.id = p.user_id
				WHERE u.active = true
				  AND u.created_at >= CURRENT_DATE - INTERVAL '30 days'
				GROUP BY u.id, u.username, u.email
				HAVING COUNT(p.id) > 0
				ORDER BY last_post_date DESC
				LIMIT 100
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					// Process results
				}
			}
		}
	}
	// end::text-blocks-intro[]

	// tag::complex-join[]
	/**
	 * Complex JOIN query with aggregations using Text Blocks
	 */
	public void multiTableJoinWithAggregation(Connection connection) throws Exception {
		String query = """
				SELECT 
					c.category_id,
					c.category_name,
					COUNT(DISTINCT p.product_id) as product_count,
					SUM(oi.quantity) as total_sold,
					AVG(oi.unit_price) as avg_price,
					MAX(o.order_date) as last_order_date
				FROM categories c
				INNER JOIN products p ON c.category_id = p.category_id
				INNER JOIN order_items oi ON p.product_id = oi.product_id
				INNER JOIN orders o ON oi.order_id = o.order_id
				WHERE o.order_date >= ? AND o.status = 'completed'
				GROUP BY c.category_id, c.category_name
				HAVING SUM(oi.quantity) > 100
				ORDER BY total_sold DESC
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.setString(1, "2024-01-01");
			stmt.executeQuery();
		}
	}
	// end::complex-join[]

	// tag::cte-query[]
	/**
	 * Common Table Expression (CTE) with recursive query using Text Blocks
	 */
	public void recursiveCTEQuery(Connection connection) throws Exception {
		String query = """
				WITH RECURSIVE org_hierarchy AS (
					-- Base case: top-level departments
					SELECT 
						dept_id,
						dept_name,
						parent_dept_id,
						1 as level,
						CAST(dept_name AS VARCHAR) as hierarchy_path
					FROM departments
					WHERE parent_dept_id IS NULL
					
					UNION ALL
					
					-- Recursive case: child departments
					SELECT 
						d.dept_id,
						d.dept_name,
						d.parent_dept_id,
						oh.level + 1,
						CONCAT(oh.hierarchy_path, ' -> ', d.dept_name)
					FROM departments d
					INNER JOIN org_hierarchy oh ON d.parent_dept_id = oh.dept_id
					WHERE oh.level < 5
				)
				SELECT 
					dept_id,
					dept_name,
					level,
					hierarchy_path
				FROM org_hierarchy
				ORDER BY level, hierarchy_path
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.executeQuery();
		}
	}
	// end::cte-query[]

	// tag::window-functions[]
	/**
	 * Window functions for analytics using Text Blocks
	 */
	public void windowFunctionsAnalytics(Connection connection) throws Exception {
		String query = """
				SELECT 
					employee_id,
					employee_name,
					department_id,
					salary,
					ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) as dept_salary_rank,
					RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as dept_salary_rank_dense,
					LAG(salary, 1, 0) OVER (PARTITION BY department_id ORDER BY salary DESC) as prev_salary,
					LEAD(salary, 1, 0) OVER (PARTITION BY department_id ORDER BY salary DESC) as next_salary,
					ROUND(AVG(salary) OVER (PARTITION BY department_id), 2) as dept_avg_salary,
					ROUND(100.0 * salary / SUM(salary) OVER (PARTITION BY department_id), 2) as pct_of_dept_payroll
				FROM employees
				WHERE active = true
				ORDER BY department_id, salary DESC
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.executeQuery();
		}
	}
	// end::window-functions[]

	// tag::case-expressions[]
	/**
	 * Complex CASE expressions with business logic using Text Blocks
	 */
	public void complexCaseExpressions(Connection connection) throws Exception {
		String query = """
				SELECT 
					product_id,
					product_name,
					category_id,
					current_price,
					CASE 
						WHEN current_price > 1000 THEN 'Premium'
						WHEN current_price > 500 THEN 'High-End'
						WHEN current_price > 100 THEN 'Mid-Range'
						ELSE 'Budget'
					END as price_tier,
					CASE category_id
						WHEN 1 THEN 'Electronics'
						WHEN 2 THEN 'Clothing'
						WHEN 3 THEN 'Home & Garden'
						WHEN 4 THEN 'Sports'
						ELSE 'Other'
					END as category_name,
					CASE 
						WHEN stock_quantity = 0 THEN 'Out of Stock'
						WHEN stock_quantity < 50 THEN 'Low Stock'
						WHEN stock_quantity < 200 THEN 'In Stock'
						ELSE 'Well Stocked'
					END as stock_status
				FROM products
				WHERE active = true
				ORDER BY category_id, current_price DESC
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.executeQuery();
		}
	}
	// end::case-expressions[]

	// tag::subquery-aggregation[]
	/**
	 * Nested subqueries with aggregation using Text Blocks
	 */
	public void nestedSubqueryWithAggregation(Connection connection) throws Exception {
		String query = """
				SELECT 
					main.customer_id,
					main.customer_name,
					main.total_orders,
					main.total_amount_spent,
					ROUND(main.total_amount_spent / main.total_orders, 2) as avg_order_value,
					comparison.avg_customer_spend,
					CASE 
						WHEN main.total_amount_spent > comparison.avg_customer_spend THEN 'Above Average'
						WHEN main.total_amount_spent < comparison.avg_customer_spend THEN 'Below Average'
						ELSE 'Average'
					END as spending_category
				FROM (
					SELECT 
						c.customer_id,
						c.customer_name,
						COUNT(o.order_id) as total_orders,
						SUM(o.total_amount) as total_amount_spent
					FROM customers c
					LEFT JOIN orders o ON c.customer_id = o.customer_id
					WHERE c.active = true
					GROUP BY c.customer_id, c.customer_name
				) main
				CROSS JOIN (
					SELECT AVG(customer_spend) as avg_customer_spend
					FROM (
						SELECT c.customer_id, SUM(o.total_amount) as customer_spend
						FROM customers c
						LEFT JOIN orders o ON c.customer_id = o.customer_id
						GROUP BY c.customer_id
					) spend_summary
				) comparison
				WHERE main.total_orders > 0
				ORDER BY main.total_amount_spent DESC
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.executeQuery();
		}
	}
	// end::subquery-aggregation[]

	// tag::union-queries[]
	/**
	 * UNION queries combining multiple result sets using Text Blocks
	 */
	public void unionQueryExample(Connection connection) throws Exception {
		String query = """
				SELECT 
					'Customer' as entity_type,
					customer_id as entity_id,
					customer_name as entity_name,
					email,
					created_at,
					'Active' as status
				FROM customers
				WHERE active = true
				
				UNION ALL
				
				SELECT 
					'Vendor' as entity_type,
					vendor_id as entity_id,
					vendor_name as entity_name,
					vendor_email as email,
					vendor_created_at as created_at,
					CASE WHEN vendor_active = true THEN 'Active' ELSE 'Inactive' END as status
				FROM vendors
				
				UNION ALL
				
				SELECT 
					'Partner' as entity_type,
					partner_id as entity_id,
					partner_name as entity_name,
					partner_email as email,
					partner_registered_at as created_at,
					'Active' as status
				FROM partners
				WHERE partner_verified = true
				
				ORDER BY created_at DESC, entity_type
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.executeQuery();
		}
	}
	// end::union-queries[]

	// tag::dynamic-filtering[]
	/**
	 * Query with dynamic filtering conditions using Text Blocks
	 * 
	 * Note: For dynamic queries in production, use parameterized PreparedStatements
	 * as shown here to prevent SQL injection.
	 */
	public void dynamicFilteringQuery(Connection connection) throws Exception {
		String baseQuery = """
				SELECT 
					t.transaction_id,
					t.account_id,
					t.transaction_date,
					t.amount,
					t.transaction_type,
					t.description,
					a.account_name,
					a.account_balance
				FROM transactions t
				INNER JOIN accounts a ON t.account_id = a.account_id
				WHERE 1=1
				""";

		StringBuilder query = new StringBuilder(baseQuery);

		// Add dynamic filters (demonstrate query building)
		boolean needsDateFilter = true;
		boolean needsAmountFilter = true;
		boolean needsTypeFilter = true;

		if (needsDateFilter) {
			query.append("\n\tAND t.transaction_date >= ?");
		}
		if (needsAmountFilter) {
			query.append("\n\tAND t.amount > ?");
		}
		if (needsTypeFilter) {
			query.append("\n\tAND t.transaction_type IN (?, ?)");
		}

		query.append("\nORDER BY t.transaction_date DESC");

		try (PreparedStatement stmt = connection.prepareStatement(query.toString())) {
			int paramIndex = 1;
			if (needsDateFilter) {
				stmt.setString(paramIndex++, "2024-01-01");
			}
			if (needsAmountFilter) {
				stmt.setDouble(paramIndex++, 100.0);
			}
			if (needsTypeFilter) {
				stmt.setString(paramIndex++, "DEBIT");
				stmt.setString(paramIndex++, "TRANSFER");
			}
			stmt.executeQuery();
		}
	}
	// end::dynamic-filtering[]

	// tag::performance-tip[]
	/**
	 * Performance tip: Using Text Blocks with prepared statements
	 * 
	 * Text blocks make complex SQL more maintainable, but always use
	 * PreparedStatements with parameter binding (?) for:
	 * - SQL injection prevention
	 * - Query plan caching
	 * - Better performance
	 */
	public void performanceOptimizedQuery(Connection connection) throws Exception {
		// Good: Parameterized query with Text Block for readability
		String query = """
				SELECT 
					p.product_id,
					p.product_name,
					p.current_price,
					COUNT(ri.review_id) as review_count,
					ROUND(AVG(ri.rating), 2) as avg_rating
				FROM products p
				LEFT JOIN reviews ri ON p.product_id = ri.product_id
				WHERE p.category_id = ?
				  AND p.current_price BETWEEN ? AND ?
				  AND p.active = true
				GROUP BY p.product_id, p.product_name, p.current_price
				HAVING COUNT(ri.review_id) > ?
				ORDER BY avg_rating DESC, review_count DESC
				LIMIT ?
				""";

		try (PreparedStatement stmt = connection.prepareStatement(query)) {
			stmt.setInt(1, 5); // category_id
			stmt.setDouble(2, 50.0); // min_price
			stmt.setDouble(3, 500.0); // max_price
			stmt.setInt(4, 10); // min_review_count
			stmt.setInt(5, 100); // limit
			stmt.executeQuery();
		}
	}
	// end::performance-tip[]

}
