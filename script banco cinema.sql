create database cinema;

use cinema;

create table filmes (
    id bigint,
	titulo_obra varchar (80),
    id_genero varchar(20),
    nacionalidade varchar (25),
    pais varchar(80),
    lancamento date,
    distribuidora varchar (50),
    origem_distribuidora varchar (50),
    constraint filmes_id primary key (id)
);

create table sala (
	id bigint,
    ano_exibicao int,
    semana_exibicao varchar (20),
    salas_na_semana int,
    publico_na_semana int,
    renda double,
    constraint fk_id foreign key (id) references filmes (id)
);

create table genero (
	id_genero varchar (20),
    genero varchar (25),
    constraint pk_id_genero primary key (id_genero)
);

create table pais (
    pais varchar (80)
);

ALTER TABLE filmes
ADD CONSTRAINT fk_id_genero foreign key (id_genero) references genero (id_genero);




--CONSULTAS DO POWERBI

--MONTAR A TABELA POWER BI TOP 10 RENDA FILMES

select filmes.titulo_obra, sum(sala.renda) as rendaMAX from filmes inner join sala on sala.id=filmes.id group by filmes.titulo_obra order by rendaMAX desc limit 10

--MONTAR A TABELA POWER BI TOP 10 RENDA ESTÚDIO

select filmes.distribuidora, sum(sala.renda) as rendaMAX from filmes inner join sala on sala.id=filmes.id group by filmes.distribuidora order by rendaMAX desc limit 10