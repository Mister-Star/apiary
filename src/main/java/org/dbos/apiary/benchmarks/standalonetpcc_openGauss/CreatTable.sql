drop FOREIGN table CONFIG;
drop FOREIGN table NEW_ORDER;
drop FOREIGN table ORDER_LINE;
drop FOREIGN table OORDER;
drop FOREIGN table HISTORY;
drop FOREIGN table CUSTOMER;
drop FOREIGN table STOCK;
drop FOREIGN table ITEM;
drop FOREIGN table DISTRICT;
drop FOREIGN table WAREHOUSE;

create FOREIGN table CONFIG (
                              cfg_name    varchar(30) primary key,
                              cfg_value   varchar(50)
);

create FOREIGN table WAREHOUSE (
                                __apiaryID__ varchar(100) primary key,
                                w_id        integer   not null,
                                 w_ytd       decimal(12,2),
                                 w_tax       decimal(4,4),
                                 w_name      varchar(10),
                                 w_street_1  varchar(20),
                                 w_street_2  varchar(20),
                                 w_city      varchar(20),
                                 w_state     char(2),
                                 w_zip       char(9)
);

create FOREIGN table DISTRICT (
                                __apiaryID__ varchar(100) primary key,
                                d_w_id       integer       not null,
                                d_id         integer       not null,
                                d_ytd        decimal(12,2),
                                d_tax        decimal(4,4),
                                d_next_o_id  integer,
                                d_name       varchar(10),
                                d_street_1   varchar(20),
                                d_street_2   varchar(20),
                                d_city       varchar(20),
                                d_state      char(2),
                                d_zip        char(9)
);

create FOREIGN table CUSTOMER (
                          __apiaryID__ varchar(100) primary key,
                                c_w_id         integer        not null,
                                c_d_id         integer        not null,
                                c_id           integer        not null,
                                c_discount     decimal(4,4),
                                c_credit       char(2),
                                c_last         varchar(16),
                                c_first        varchar(16),
                                c_credit_lim   decimal(12,2),
                                c_balance      decimal(12,2),
                                c_ytd_payment  decimal(12,2),
                                c_payment_cnt  integer,
                                c_delivery_cnt integer,
                                c_street_1     varchar(20),
                                c_street_2     varchar(20),
                                c_city         varchar(20),
                                c_state        char(2),
                                c_zip          char(9),
                                c_phone        char(16),
                                c_since        timestamp,
                                c_middle       char(2),
                                c_data         varchar(500)
);

create FOREIGN table HISTORY (
                         __apiaryID__ varchar(100) primary key,
                               hist_id  integer,
                               h_c_id   integer,
                               h_c_d_id integer,
                               h_c_w_id integer,
                               h_d_id   integer,
                               h_w_id   integer,
                               h_date   timestamp,
                               h_amount decimal(6,2),
                               h_data   varchar(24)
);

create FOREIGN table NEW_ORDER (
                           __apiaryID__ varchar(100) primary key,
                                 no_w_id  integer   not null,
                                 no_d_id  integer   not null,
                                 no_o_id  integer   not null
);

create FOREIGN table OORDER (
                        __apiaryID__ varchar(100) primary key,
                              o_w_id       integer      not null,
                              o_d_id       integer      not null,
                              o_id         integer      not null,
                              o_c_id       integer,
                              o_carrier_id integer,
                              o_ol_cnt     integer,
                              o_all_local  integer,
                              o_entry_d    timestamp
);

create FOREIGN table ORDER_LINE (
                            __apiaryID__ varchar(100) primary key,
                                  ol_w_id         integer   not null,
                                  ol_d_id         integer   not null,
                                  ol_o_id         integer   not null,
                                  ol_number       integer   not null,
                                  ol_i_id         integer   not null,
                                  ol_delivery_d   timestamp,
                                  ol_amount       decimal(6,2),
                                  ol_supply_w_id  integer,
                                  ol_quantity     integer,
                                  ol_dist_info    char(24)
);

create FOREIGN table ITEM (
                      __apiaryID__ varchar(100) primary key,
                            i_id     integer      not null,
                            i_name   varchar(24),
                            i_price  decimal(5,2),
                            i_data   varchar(50),
                            i_im_id  integer
);

create FOREIGN table STOCK (
                       __apiaryID__ varchar(100) primary key,
                             s_w_id       integer       not null,
                             s_i_id       integer       not null,
                             s_quantity   integer,
                             s_ytd        integer,
                             s_order_cnt  integer,
                             s_remote_cnt integer,
                             s_data       varchar(50),
                             s_dist_01    char(24),
                             s_dist_02    char(24),
                             s_dist_03    char(24),
                             s_dist_04    char(24),
                             s_dist_05    char(24),
                             s_dist_06    char(24),
                             s_dist_07    char(24),
                             s_dist_08    char(24),
                             s_dist_09    char(24),
                             s_dist_10    char(24)
);


