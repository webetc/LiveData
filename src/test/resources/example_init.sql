
CREATE TABLE person (
  id int(11) NOT NULL AUTO_INCREMENT,
  name varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE phone (
  id int(11) NOT NULL AUTO_INCREMENT,
  userId int(11) NOT NULL,
  phoneNumber varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


insert into person(name) values
("Bob"),
("Chris");


insert into phone(userId, phoneNumber)
select p.id, '555-555-0001' from person p where p.name = 'Bob';

insert into phone(userId, phoneNumber)
select p.id, '555-555-0002' from person p where p.name = 'Chris';

