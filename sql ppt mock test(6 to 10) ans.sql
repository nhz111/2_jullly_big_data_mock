/*6.  Retrieve all the customers from the "Customers" table whose age is greater than 25 and have made at least one purchase*/

SELECT *
FROM Customers
WHERE age > 25
  AND customer_id IN (
    SELECT customer_id
    FROM Purchases
    GROUP BY customer_id
    HAVING COUNT(*) > 0
  );

/*7. Find the total number of orders placed by each customer and display the results in descending order of the number of orders.*/

SELECT customer_id, COUNT(*) AS order_count
FROM Orders
GROUP BY customer_id
ORDER BY order_count DESC;


/*8. Retrieve the names of all products that are currently out of stock from the "Products" table.*/

SELECT name
FROM Products
WHERE stock = 0;

/*9. Calculate the average price of all products in each category and display the results along with the category name.*/

SELECT category_name, AVG(price) AS average_price
FROM Products
GROUP BY category_name;

/*10. 10. Retrieve the top 5 customers who have spent the highest total amount on purchases.*/

SELECT customer_id, SUM(amount) AS total_amount
FROM Purchases
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 5;