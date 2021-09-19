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
import org.springframework.core.io.FileSystemResource;

import com.telsuko.SpringBatchCSVtoDB.model.Student;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfig2 
{
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private JobBuilderFactory job2BuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public FlatFileItemReader<Student> readFromCsv2()
	{
		FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();
		reader.setResource(new FileSystemResource("C://Users/c.h.manasa chowdary/Desktop/inputFile2.csv"));
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
	public JdbcBatchItemWriter<Student> writerIntoDB2()
	{
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
//		writer.setSql("insert into test (id,name,m1,m2) values(:id,:name,:m1,:m2)");
		writer.setSql("update test set name=:name, m1=:m1, m2=:m2 where :id=id");
		writer.setAssertUpdates(false);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}
	
	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB3()
	{
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("insert into test2 (id,name,m1,m2) values(:id,:name,:m1,:m2)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}
	
	@Bean
	public Step step2()
	{
		return stepBuilderFactory.get("step2").<Student,Student>chunk(3)
		.reader(readFromCsv2()).writer(writerIntoDB2()).build();
	}
	
	@Bean
	public Step step3()
	{
		return stepBuilderFactory.get("step3").<Student,Student>chunk(3)
		.reader(readFromCsv2()).writer(writerIntoDB3()).build();
	}
	
	@Bean
	public Job job2()
	{
		return job2BuilderFactory.get("job2").start(step2()).on("*").to(step3()).end().build();
	}
}
