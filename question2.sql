select c.category_name, sum(prifd.amount) gross
from category c
INNER JOIN (select * from
film_category d INNER JOIN (select * from
film f INNER JOIN (select * from
inventory i INNER JOIN (select * from
rental r INNER JOIN payment p ON p.rental_id = r.rental_id) pr ON pr.inventory_id = i.inventory_id) pri ON pri.film_id = f.film_id) prif ON prif.film_id = d.film_id) prifd
ON c.category_id = prifd.category_id
group by c.category_name
order by gross desc limit 5
