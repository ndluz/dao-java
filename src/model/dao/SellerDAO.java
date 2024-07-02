package model.dao;

import java.util.List;

import model.entities.Seller;

public interface SellerDAO {
	void insert(Seller seller);
	void update(Seller seller);
	void delete(Integer id);
	Seller findById(Integer id);
	List<Seller> findAll();
}
