insert into cp1.shop (id, name) values (1, 'shop-1');

insert into cp1.product (id, name, shop_id) values (1, 'product-1', 1);
insert into cp1.product (id, name, shop_id) values (2, 'product-2', 1);

insert into cp1.characteristic (id, characteristic_name) values (1, 'characteristic-1');
insert into cp1.characteristic (id, characteristic_name) values (2, 'characteristic-2');
insert into cp1.characteristic (id, characteristic_name) values (3, 'characteristic-3');

insert into cp1.product_characteristics (product_id, characteristic_id) values (1, 1);
insert into cp1.product_characteristics (product_id, characteristic_id) values (1, 2);
insert into cp1.product_characteristics (product_id, characteristic_id) values (2, 2);
insert into cp1.product_characteristics (product_id, characteristic_id) values (2, 3);