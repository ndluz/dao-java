package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDAO;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDAO {
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Seller seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
			statement.setInt(1, id);
			
			
			resultSet = statement.executeQuery();
			
			if (resultSet.next()) {
				// need to associate both objects Department and Seller
				Department department = instanciateDepartment(resultSet);
				
				Seller seller = instanciateSeller(resultSet, department);
				return seller;
			}
			
			return null;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}

	private Seller instanciateSeller(ResultSet resultSet, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(resultSet.getInt("Id"));
		seller.setName(resultSet.getString("Name"));
		seller.setEmail(resultSet.getString("Email"));
		seller.setBaseSalary(resultSet.getDouble("BaseSalary"));
		seller.setBirthDate(resultSet.getDate("BirthDate"));
		seller.setDepartment(department);
		return seller;
	}

	private Department instanciateDepartment(ResultSet resultSet) throws SQLException {
		Department department = new Department();
		department.setId(resultSet.getInt("Id"));
		department.setName(resultSet.getString("name"));
		return department;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = this.conn.prepareStatement(
					 "SELECT seller.*, department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY NAME");
			
			resultSet = statement.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> depMap = new HashMap<>();
			
			while(resultSet.next()) {
				Department dep = depMap.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = this.instanciateDepartment(resultSet);
					depMap.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = this.instanciateSeller(resultSet, dep);
				sellers.add(seller);
			}
			
			return sellers;
			
		} catch( SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = this.conn.prepareStatement(
					 "SELECT seller.*, department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY NAME");
			
			statement.setInt(1, department.getId());
			resultSet = statement.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> depMap = new HashMap<>();
			
			while(resultSet.next()) {
				Department dep = depMap.get(resultSet.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = this.instanciateDepartment(resultSet);
					depMap.put(resultSet.getInt("DepartmentId"), dep);
				}
				
				Seller seller = this.instanciateSeller(resultSet, dep);
				sellers.add(seller);
			}
			
			return sellers;
			
		} catch( SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
	}
}
