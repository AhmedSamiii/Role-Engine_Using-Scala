# Functional Programming with Scala - Project Documentation

## 1. Project Overview
The project aims to develop a rule engine in Scala for a retail store. This rule engine qualifies orders' transactions for discounts based on a set of qualifying rules. Additionally, it automatically calculates the proper discount based on specific calculation rules.

## 2. Rules Required
The project implements the following qualifying rules and calculation rules:

### Qualifying Rules:
1. **Less Than 30 Days Remaining for Product Expiry:**
   - If the product has less than 30 days remaining to expire from the day of the transaction, it qualifies for a discount.

2. **Cheese and Wine Products On Sale:**
   - Cheese products qualify for a 10% discount.
   - Wine products qualify for a 5% discount.

3. **Special Discount on 23rd of March:**
   - Products bought on the 23rd of March qualify for a 50% discount.

4. **Quantity-Based Discount:**
   - If a customer buys more than 5 of the same product:
     - 6-9 units qualify for a 5% discount.
     - 10-14 units qualify for a 7% discount.
     - More than 15 units qualify for a 10% discount.

5. **Sales Through App:**
   - Sales made through the app qualify for a special discount based on the quantity rounded up to the nearest multiple of 5.

6. **Sales Using Visa Cards:**
   - Sales made using Visa cards qualify for a 5% discount.

### Calculation Rules:
- Transactions that didn’t qualify for any discount will have a 0% discount.
- Transactions that qualify for more than one discount will get the top 2 and get their average.

## 3. Approach Followed in the Code
### Core Functional Logic:
- Utilization of pure functional programming principles.
- Usage of `vals` instead of `vars`.
- Avoidance of mutable data structures and loops.
- Implementation of pure functions where:
  - Output depends solely on input.
  - Input to the function doesn’t get mutated.
  - Has a predictable behavior with no side effects.

### Code Organization:
- The code is well-commented to explain each functionality.
- It adheres to clean code principles for readability and self-explanatory nature.
- Separate functions are defined for each qualifying rule and calculation rule.

### Additional Functionalities:
- Logging of engine's events in a log file named "rules_engine.log".
- Data processing from raw input to the final result, including database insertion.
- Handling of new requirements discussed in the meeting with the head of sales:
  - Discount rule for sales made through the App.
  - Discount rule for sales made using Visa cards.
