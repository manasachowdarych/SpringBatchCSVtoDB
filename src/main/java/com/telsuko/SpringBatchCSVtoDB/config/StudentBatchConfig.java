package com.telsuko.SpringBatchCSVtoDB.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.telsuko.SpringBatchCSVtoDB.model.Student;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfig 
{
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public FlatFileItemReader<Student> readFromCsv()
	{
		FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();
		//reader.setResource(new FileSystemResource("C://Users/c.h.manasa chowdary/Desktop/inputFile1.csv"));
		reader.setResource(new ClassPathResource("inputFile1.csv"));
		reader.setLinesToSkip(1);
		reader.setLineMapper(new DefaultLineMapper<Student>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
				{
					setNames(Student.fields());
				}
			});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
				{
					setTargetType(Student.class);
				}
			});
		}
	});	
	return reader;
}
	
	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB()
	{
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("insert into test (id,name,m1,m2) values(:id,:name,:m1,:m2)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}
	
	
	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB1()
	{
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("insert into test1 (id,name,m1,m2) values(:id,:name,:m1,:m2)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}
	
	@Bean
	public Step step()
	{
		return stepBuilderFactory.get("step").<Student,Student>chunk(3)
		.reader(readFromCsv()).writer(writerIntoDB()).build();
	}
	
	@Bean
	public Step step1()
	{
		return stepBuilderFactory.get("step1").<Student,Student>chunk(3)
		.reader(readFromCsv()).writer(writerIntoDB1()).build();
	}
	@Bean
	public Job job()
	{
		return jobBuilderFactory.get("job").start(step()).on("*").to(step1()).end().build();
	}
}
