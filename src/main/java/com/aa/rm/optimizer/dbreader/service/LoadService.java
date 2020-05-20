package com.aa.rm.optimizer.dbreader.service;

import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class LoadService
{
    @Autowired
    protected JdbcTemplate jdbcTemplate;

//    @Autowired
//    protected S3Service s3Service;
//
//    @Autowired
//    protected GigaSpace gigaSpace;

    @Value( "${space.batch.load.size}" )
    protected int spaceBatchLoadSize;

    // Logic to load data into GigaSpace cache goes here
    // To be implemented by the extend class
    //
    public abstract void loadData(String fileName);

    // Return the Java Class we're feeding into the space
    // To be implemented by the extend class
    //
    public abstract Class getSpaceClass();

    // Clear space object that returns by getSpaceClass()
    //
    public void clearData() throws Exception
    {
//        SQLQuery query = new SQLQuery(getSpaceClass(), "spaceId != ?");
//        query.setParameter(1, null);
//        gigaSpace.clear(query);
    }
}
