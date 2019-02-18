package com.cg.payroll.daoservices;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.cg.payroll.beans.Associate;
import com.cg.payroll.beans.BankDetails;
import com.cg.payroll.beans.Salary;
import com.cg.payroll.util.PayrollDBUtil;

public class AssociateDAOImpl implements AssociateDAO{

	private static Connection con = PayrollDBUtil.getDBConnection();

	@Override
	public Associate save(Associate associate) {
		try {
			con.setAutoCommit(false);

			PreparedStatement pstmt1 = con.prepareStatement("insert into Associate(associateId,yearlyInvestmentUnder80C, firstName, lastName, department, designation,pancard, emailId) values(ASSOCIATE_ID_SEQ.NEXTVAL,?,?,?,?,?,?,?)");
			pstmt1.setInt(1, associate.getYearlyInvestmentUnder80C());
			pstmt1.setString(2, associate.getFirstName());
			pstmt1.setString(3, associate.getLastName());
			pstmt1.setString(4, associate.getDepartment());
			pstmt1.setString(5, associate.getDesignation());
			pstmt1.setString(6, associate.getPancard());
			pstmt1.setString(7, associate.getEmailId());

			pstmt1.executeUpdate();

			PreparedStatement pstmt2  = con.prepareStatement("select max(associateId) from Associate");
			ResultSet rs = pstmt2.executeQuery();
			rs.next();
			int associateId = rs.getInt(1);

			PreparedStatement pstmt3 = con.prepareStatement("insert into Salary(associateId,basicSalary,epf,companyPf) values(?,?,?,?)");
			pstmt3.setInt(1, associateId);
			pstmt3.setInt(2, associate.getSalary().getBasicSalary());
			pstmt3.setInt(3, associate.getSalary().getEpf());
			pstmt3.setInt(4, associate.getSalary().getCompanyPf());
			pstmt3.executeUpdate();

			PreparedStatement pstmt4 = con.prepareStatement("insert into BankDetails(associateId,accountNumber,bankName,ifscCode) values(?,?,?,?)");
			pstmt4.setInt(1, associateId);
			pstmt4.setInt(2, associate.getBankDetails().getAccountNumber());
			pstmt4.setString(3, associate.getBankDetails().getBankName());
			pstmt4.setString(4, associate.getBankDetails().getIfscCode());
			pstmt4.executeUpdate();
			associate.setAssociateId(associateId);

			con.commit();
		}catch (SQLException e) {
			e.printStackTrace();
			try {
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		return associate;
	}

	@Override
	public boolean update(Associate associate) {
		return false;
	}

	@Override
	public Associate findOne(int associateId) {
		try {
			PreparedStatement pstmt1 = con.prepareStatement("select * from Associate where associateId="+associateId);
			ResultSet associateRs=pstmt1.executeQuery();
			if(associateRs.next()) {
				int yearlyInvestmentUnder80C=associateRs.getInt("yearlyInvestmentUnder80C");
				String firstName=associateRs.getString("firstName"); 
				String lastName=associateRs.getString("lastName");
				String department=associateRs.getString("department");
				String designation=associateRs.getString("designation");
				String pancard=associateRs.getString("pancard");
				String emailId=associateRs.getString("emailId");

				Associate associate = new Associate(associateId, yearlyInvestmentUnder80C, firstName, lastName, department, designation, pancard, emailId, null, null);

				PreparedStatement pstmt2 = con.prepareStatement("select * from Salary where associateId="+associateId);
				ResultSet salaryRs=pstmt2.executeQuery();
				salaryRs.next();
				int basicSalary=salaryRs.getInt("basicSalary");
				int hra=salaryRs.getInt("hra");
				int conveyenceAllowance=salaryRs.getInt("conveyenceAllowance");
				int otherAllowance=salaryRs.getInt("otherAllowance");
				int personalAllowance=salaryRs.getInt("personalAllowance");
				int monthlyTax=salaryRs.getInt("monthlyTax");
				int epf=salaryRs.getInt("epf");
				int companyPf=salaryRs.getInt("companyPf");
				int grossSalary=salaryRs.getInt("grossSalary");
				int netSalary=salaryRs.getInt("netSalary");

				Salary salary = new Salary(basicSalary, hra, conveyenceAllowance, otherAllowance, personalAllowance, monthlyTax, epf, companyPf, grossSalary, netSalary);
				associate.setSalary(salary);

				PreparedStatement pstmt3 = con.prepareStatement("select * from BankDetails where associateId="+associateId);
				ResultSet bankDetailsRs=pstmt3.executeQuery();
				bankDetailsRs.next();

				associate.setBankDetails(new BankDetails(bankDetailsRs.getInt("accountNumber"), bankDetailsRs.getString("bankName"), bankDetailsRs.getString("ifscCode")));
				return associate;
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Associate> findAll() {
		ArrayList<Associate> associates = new ArrayList<Associate>();
		try {
			PreparedStatement pstmt1 = con.prepareStatement("select * from Associate ");
			ResultSet associateRs=pstmt1.executeQuery();
			if(associateRs.next()) {
				int associateId=associateRs.getInt("associateId");
				
				Associate associate = new Associate(associateId, associateRs.getInt("yearlyInvestmentUnder80C"), associateRs.getString("firstName"), associateRs.getString("lastName"), associateRs.getString("department"), associateRs.getString("designation"), associateRs.getString("pancard"), associateRs.getString("emailId"), null, null);

				PreparedStatement pstmt2 = con.prepareStatement("select * from Salary where associateId="+associateId);
				ResultSet salaryRs=pstmt2.executeQuery();
				salaryRs.next();
				
				Salary salary = new Salary(salaryRs.getInt("basicSalary"), salaryRs.getInt("hra"), salaryRs.getInt("conveyenceAllowance"), salaryRs.getInt("otherAllowance"), salaryRs.getInt("personalAllowance"), salaryRs.getInt("monthlyTax"), salaryRs.getInt("epf"), salaryRs.getInt("companyPf"), salaryRs.getInt("grossSalary"), salaryRs.getInt("netSalary"));
				associate.setSalary(salary);

				PreparedStatement pstmt3 = con.prepareStatement("select * from BankDetails where associateId="+associateId);
				ResultSet bankDetailsRs=pstmt3.executeQuery();
				bankDetailsRs.next();

				associate.setBankDetails(new BankDetails(bankDetailsRs.getInt("accountNumber"), bankDetailsRs.getString("bankName"), bankDetailsRs.getString("ifscCode")));
				associates.add(associate);
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return associates;
	}
}
