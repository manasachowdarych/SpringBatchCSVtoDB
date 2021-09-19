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
public class StudentBatchConfig3 {
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public FlatFileItemReader<Student> readFromCsv3() {
		FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();
		//reader.setResource(new FileSystemResource("C://Users/c.h.manasa chowdary/Desktop/inputFile3.csv"));
		reader.setResource(new ClassPathResource("inputFile3.csv"));
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
	public JdbcBatchItemWriter<Student> writerIntoDB4() {
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("delete from test where id=:id and name=:name and m1=:m1 and m2=:m2");
		writer.setAssertUpdates(false);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}

	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB5() {
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("delete from test1 where id=:id and name=:name and m1=:m1 and m2=:m2");
		writer.setAssertUpdates(false);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}

	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB6() {
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("delete from test2 where id=:id and name=:name and m1=:m1 and m2=:m2");
		writer.setAssertUpdates(false);
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}

	@Bean
	public JdbcBatchItemWriter<Student> writerIntoDB7() {
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setDataSource(dataSource);
		writer.setSql("insert into test3 (id,name,m1,m2) values(:id,:name,:m1,:m2)");
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
	}

	@Bean
	public Step step4() {
		return stepBuilderFactory.get("step4").<Student, Student>chunk(3).reader(readFromCsv3()).writer(writerIntoDB4())
				.build();
	}

	@Bean
	public Step step5() {
		return stepBuilderFactory.get("step5").<Student, Student>chunk(3).reader(readFromCsv3()).writer(writerIntoDB5())
				.build();
	}

	@Bean
	public Step step6() {
		return stepBuilderFactory.get("step6").<Student, Student>chunk(3).reader(readFromCsv3()).writer(writerIntoDB6())
				.build();
	}

	@Bean
	public Step step7() {
		return stepBuilderFactory.get("step7").<Student, Student>chunk(3).reader(readFromCsv3()).writer(writerIntoDB7())
				.build();
	}

	@Bean
	public Job job3() {
		return jobBuilderFactory.get("job3").start(step4()).on("*").to(step5()).on("*").to(step6()).on("*").to(step7())
				.end().build();
	}
}
