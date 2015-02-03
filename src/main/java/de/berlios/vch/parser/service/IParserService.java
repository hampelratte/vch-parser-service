package de.berlios.vch.parser.service;

import java.net.URI;
import java.util.Set;

import de.berlios.vch.parser.IOverviewPage;
import de.berlios.vch.parser.IWebPage;
import de.berlios.vch.parser.IWebParser;

public interface IParserService {
    public IWebPage parse(URI vchUri) throws Exception;
    
    public IOverviewPage getParserOverview() throws Exception;
    
    public Set<IWebParser> getParsers();
    
    public IWebParser getParser(String id);
}
