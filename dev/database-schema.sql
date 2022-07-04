CREATE SCHEMA cp1;

CREATE TABLE `cp1`.`shop` (
  `id` INT NOT NULL,
  `name` VARCHAR(45) NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `cp1`.`product` (
  `id` INT NOT NULL,
  `name` VARCHAR(45) NULL,
  `shop_id` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `product_shop_fk_idx` (`shop_id` ASC) VISIBLE,
  CONSTRAINT `product_shop_fk` FOREIGN KEY (`shop_id`) REFERENCES `cp1`.`shop` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
);

CREATE TABLE `cp1`.`characteristic` (
  `id` INT NOT NULL,
  `characteristic_name` VARCHAR(45) NULL,
  PRIMARY KEY (`id`));
  
CREATE TABLE `cp1`.`product_characteristics` (
  `product_id` INT NOT NULL,
  `characteristic_id` INT NOT NULL,
  INDEX `product_characteristics_p_fk_idx` (`product_id` ASC) VISIBLE,
  INDEX `product_characteristics_c_fk_idx` (`characteristic_id` ASC) VISIBLE,
  CONSTRAINT `product_characteristics_p_fk`
    FOREIGN KEY (`product_id`)
    REFERENCES `cp1`.`product` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `product_characteristics_c_fk`
    FOREIGN KEY (`characteristic_id`)
    REFERENCES `cp1`.`characteristic` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);