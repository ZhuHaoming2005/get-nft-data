--
-- PostgreSQL database dump
--

\restrict 4fPcOs3EvUkYfEVwTk3mBKey7wnu5Fy7VRxaNjJU7esAzezO8B38qSJHN8sIpEl

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: nft_assets_ethereum; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.nft_assets_ethereum (
    id bigint NOT NULL,
    contract_address character varying(42) NOT NULL,
    token_id numeric NOT NULL,
    token_uri text,
    image_uri text,
    token_standard character varying(10),
    first_seen_block bigint,
    created_at timestamp with time zone DEFAULT now(),
    name character varying(200),
    symbol character varying(20)
);


ALTER TABLE public.nft_assets_ethereum OWNER TO postgres;

--
-- Name: nft_assets_ethereum_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.nft_assets_ethereum_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.nft_assets_ethereum_id_seq OWNER TO postgres;

--
-- Name: nft_assets_ethereum_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.nft_assets_ethereum_id_seq OWNED BY public.nft_assets_ethereum.id;


--
-- Name: nft_assets_ethereum id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nft_assets_ethereum ALTER COLUMN id SET DEFAULT nextval('public.nft_assets_ethereum_id_seq'::regclass);


--
-- Name: nft_assets_ethereum nft_assets_ethereum_contract_address_token_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nft_assets_ethereum
    ADD CONSTRAINT nft_assets_ethereum_contract_address_token_id_key UNIQUE (contract_address, token_id);


--
-- Name: nft_assets_ethereum nft_assets_ethereum_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.nft_assets_ethereum
    ADD CONSTRAINT nft_assets_ethereum_pkey PRIMARY KEY (id);


--
-- Name: idx_nft_assets_ethereum_contract; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_nft_assets_ethereum_contract ON public.nft_assets_ethereum USING btree (contract_address);


--
-- PostgreSQL database dump complete
--

\unrestrict 4fPcOs3EvUkYfEVwTk3mBKey7wnu5Fy7VRxaNjJU7esAzezO8B38qSJHN8sIpEl

