// 0. Constraints
CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT family_id_unique IF NOT EXISTS FOR (f:Family ) REQUIRE f.id IS UNIQUE;

// 1. Create People
CREATE

// Generation 1
(p1:Person { id:'p1', first_name:'Timothy', last_name:'Cruz', sex:'Male', birth_date:date('1880-01-01'), death_date:date('1962-01-01'), education:'No formal education', works:'Farmer' }),
(p2:Person { id:'p2', first_name:'Amy', last_name:'Harolds', sex:'Female', birth_date:date('1885-01-01'), death_date:date('1966-01-01'), education:'Primary School', works:'Homemaker' }),
(p3:Person { id:'p3', first_name:'Cynthia', last_name:'Willow', sex:'Female', birth_date:date('1899-01-01'), death_date:date('1950-01-01'), education:'High School', works:'Seamstress' }),
(p4:Person { id:'p4', first_name:'Fred', last_name:'Lambert', sex:'Male', birth_date:date('1890-01-01'), death_date:date('1977-01-01'), education:'Apprenticeship', works:'Carpenter' }),

// Generation 2
(p5:Person { id:'p5', first_name:'Margaret', last_name:'Yap', sex:'Female', birth_date:date('1912-01-01'), death_date:date('2010-01-01'), education:'Nursing College', works:'Nurse' }),
(p6:Person { id:'p6', first_name:'Nick', last_name:'Young', sex:'Male', birth_date:date('1908-01-01'), death_date:date('1991-01-01'), education:'High School', works:'Postman' }),
(p7:Person { id:'p7', first_name:'Alexander', last_name:'Cruz', sex:'Male', birth_date:date('1918-01-01'), death_date:date('2003-01-01'), education:'Military Academy', works:'Navy Officer' }),
(p8:Person { id:'p8', first_name:'Jerry', last_name:'Willow', sex:'Female', birth_date:date('1920-01-01'), death_date:date('1989-01-01'), education:'Teaching College', works:'Teacher' }),
(p9:Person { id:'p9', first_name:'Emma', last_name:'Young', sex:'Female', birth_date:date('1950-01-01'), education:'Music Conservatory', works:'Musician' }),

// Generation 3
(p10:Person { id:'p10', first_name:'Jake', last_name:'Cruz', sex:'Male', birth_date:date('1945-01-01'), death_date:date('2022-01-01'), education:'PhD Physics', works:'Professor' }),
(p11:Person { id:'p11', first_name:'Taylor', last_name:'Humes', sex:'Female', birth_date:date('1955-01-01'), education:'Masters History', works:'Museum Curator' }),
(p12:Person { id:'p12', first_name:'Olivia', last_name:'Cruz', sex:'Female', birth_date:date('1960-01-01'), education:'MBA', works:'CEO' }),
(p13:Person { id:'p13', first_name:'Arjun', last_name:'Patel', sex:'Male', birth_date:date('1962-01-01'), education:'MSc Computer Science', works:'Software Architect' }),
(p14:Person { id:'p14', first_name:'Timothy', last_name:'Cruz', sex:'Male', birth_date:date('1940-01-01'), death_date:date('2020-01-01'), education:'BArch', works:'Architect' }),

// Generation 4
(p15:Person { id:'p15', first_name:'Christina', last_name:'Cruz', sex:'Female', birth_date:date('1978-01-01'), education:'Medical School', works:'Doctor' }),
(p16:Person { id:'p16', first_name:'Timothy', last_name:'Young', sex:'Male', birth_date:date('1970-01-01'), education:'Finance Degree', works:'Banker' }),
(p17:Person { id:'p17', first_name:'Cullen', last_name:'Cruz', sex:'Male', birth_date:date('1975-01-01'), education:'Art School', works:'Graphic Designer' }),
(p18:Person { id:'p18', first_name:'Marie', last_name:'Cruz', sex:'Female', birth_date:date('1973-01-01'), death_date:date('2018-01-01'), education:'Law School', works:'Lawyer' }),
(p19:Person { id:'p19', first_name:'Steve', last_name:'Cruz', sex:'Male', birth_date:date('1985-01-01'), education:'Engineering Degree', works:'Civil Engineer' }),
(p20:Person { id:'p20', first_name:'Sammie', last_name:'Chang', sex:'Female', birth_date:date('1985-01-01'), education:'PhD Chemistry', works:'Scientist' }),
(p21:Person { id:'p21', first_name:'Arham', last_name:'Chopra', sex:'Female', birth_date:date('1983-01-01'), education:'MFA', works:'Writer' }),
(p22:Person { id:'p22', first_name:'Maria', last_name:'Young', sex:'Female', birth_date:date('1988-01-01'), education:'BSc Biology', works:'Researcher' }),
(p23:Person { id:'p23', first_name:'Marvin', last_name:'Young', sex:'Male', birth_date:date('1970-01-01'), education:'Trade School', works:'Mechanic' }),
(p24:Person { id:'p24', first_name:'Jazmine', last_name:'Brooke', sex:'Female', birth_date:date('1975-01-01'), education:'Culinary School', works:'Chef' }),
(p25:Person { id:'p25', first_name:'Jane', last_name:'Howell', sex:'Female', birth_date:date('2002-01-01'), education:'High School', works:'Student' }),
(p26:Person { id:'p26', first_name:'William', last_name:'Cruz', sex:'Male', birth_date:date('2002-01-01'), education:'High School', works:'Student' }),

// Generation 5
(p27:Person { id:'p27', first_name:'Carrie', last_name:'Young', sex:'Female', birth_date:date('2001-01-01'), education:'High School', works:'Student' }),
(p28:Person { id:'p28', first_name:'Carlo', last_name:'Young', sex:'Male', birth_date:date('2003-01-01'), education:'High School', works:'Student' }),
(p29:Person { id:'p29', first_name:'Tay', last_name:'Young', sex:'Male', birth_date:date('2005-01-01'), education:'Middle School', works:'Student' }),
(p30:Person { id:'p30', first_name:'Anna', last_name:'Cruz', sex:'Female', birth_date:date('2012-01-01'), education:'Primary School', works:'Student' }),
(p31:Person { id:'p31', first_name:'Taylor', last_name:'Cruz', sex:'Female', birth_date:date('2009-01-01'), education:'Primary School', works:'Student' }),
(p32:Person { id:'p32', first_name:'Amelia', last_name:'Young', sex:'Female', birth_date:date('2001-01-01'), education:'High School', works:'Student' }),
(p33:Person { id:'p33', first_name:'Jake', last_name:'Young', sex:'Male', birth_date:date('2002-01-01'), education:'High School', works:'Student' }),
(p34:Person { id:'p34', first_name:'Timothy', last_name:'Young', sex:'Male', birth_date:date('2010-01-01'), education:'Elementary School', works:'Student' }),

// Generation 6
(p35:Person { id:'p35', first_name:'William', last_name:'Cruz', sex:'Male', birth_date:date('2021-01-01'), education:'None', works:'Infant' }),
(p36:Person { id:'p36', first_name:'Alex', last_name:'Cruz', sex:'Male', birth_date:date('2021-01-01'), education:'None', works:'Infant' }),

// 2. Create Families (Family Units)
(f1:Family { id:'f1', marriage_date:date('1910-06-01'), status:'WIDOWED' }),
(f2:Family { id:'f2', marriage_date:date('1919-09-15'), status:'WIDOWED' }),
(f3:Family { id:'f3', marriage_date:date('1935-04-20'), status:'WIDOWED' }),
(f4:Family { id:'f4', marriage_date:date('1939-11-10'), status:'WIDOWED' }),
(f5:Family { id:'f5', marriage_date:date('1969-07-07'), status:'WIDOWED' }),
(f6:Family { id:'f6', marriage_date:date('1970-05-05'), status:'WIDOWED' }),
(f7:Family { id:'f7', marriage_date:date('1984-08-20'), status:'ACTIVE' }),
(f8:Family { id:'f8', marriage_date:date('2000-02-14'), status:'ACTIVE' }),
(f9:Family { id:'f9', marriage_date:date('2008-06-10'), status:'DIVORCED', divorce_date:date('2011-01-01') }),
(f10:Family { id:'f10', marriage_date:date('2012-03-30'), status:'ACTIVE' }),
(f11:Family { id:'f11', marriage_date:date('1995-11-22'), status:'ACTIVE' }),
(f12:Family { id:'f12', marriage_date:date('2020-09-09'), status:'ACTIVE' }),

// 3. Link Parents to Families
(p1)-[:SPOUSE_OF { role:'HUSBAND' }]->(f1), (p2)-[:SPOUSE_OF {role:'WIFE'}]->(f1),
(p4)-[:SPOUSE_OF { role:'HUSBAND' }]->(f2), (p3)-[:SPOUSE_OF {role:'WIFE'}]->(f2),
(p6)-[:SPOUSE_OF { role:'HUSBAND' }]->(f3), (p5)-[:SPOUSE_OF {role:'WIFE'}]->(f3),
(p7)-[:SPOUSE_OF { role:'HUSBAND' }]->(f4), (p8)-[:SPOUSE_OF {role:'WIFE'}]->(f4),
(p14)-[:SPOUSE_OF { role:'HUSBAND' }]->(f5), (p9)-[:SPOUSE_OF {role:'WIFE'}]->(f5),
(p10)-[:SPOUSE_OF { role:'HUSBAND' }]->(f6), (p11)-[:SPOUSE_OF {role:'WIFE'}]->(f6),
(p13)-[:SPOUSE_OF { role:'HUSBAND' }]->(f7), (p12)-[:SPOUSE_OF {role:'WIFE'}]->(f7),
(p16)-[:SPOUSE_OF { role:'HUSBAND' }]->(f8), (p15)-[:SPOUSE_OF {role:'WIFE'}]->(f8),
(p19)-[:SPOUSE_OF { role:'HUSBAND' }]->(f9), (p20)-[:SPOUSE_OF {role:'WIFE'}]->(f9),
(p19)-[:SPOUSE_OF { role:'HUSBAND' }]->(f10), (p21)-[:SPOUSE_OF {role:'WIFE'}]->(f10),
(p23)-[:SPOUSE_OF { role:'HUSBAND' }]->(f11), (p24)-[:SPOUSE_OF {role:'WIFE'}]->(f11),
(p26)-[:SPOUSE_OF { role:'HUSBAND' }]->(f12), (p25)-[:SPOUSE_OF {role:'WIFE'}]->(f12),

// 4. Link Children to Families
(p7)-[:CHILD_OF]->(f1),
(p8)-[:CHILD_OF]->(f2),
(p9)-[:CHILD_OF]->(f3),
(p10)-[:CHILD_OF]->(f4), (p12)-[:CHILD_OF]->(f4), (p14)-[:CHILD_OF]->(f4),
(p22)-[:CHILD_OF]->(f5), (p23)-[:CHILD_OF]->(f5),
(p15)-[:CHILD_OF]->(f6), (p17)-[:CHILD_OF]->(f6), (p18)-[:CHILD_OF]->(f6),
(p19)-[:CHILD_OF]->(f7),
(p27)-[:CHILD_OF]->(f8), (p28)-[:CHILD_OF]->(f8), (p29)-[:CHILD_OF]->(f8),
(p26)-[:CHILD_OF]->(f9),
(p30)-[:CHILD_OF]->(f10), (p31)-[:CHILD_OF]->(f10),
(p32)-[:CHILD_OF]->(f11), (p33)-[:CHILD_OF]->(f11), (p34)-[:CHILD_OF]->(f11),
(p35)-[:CHILD_OF]->(f12), (p36)-[:CHILD_OF]->(f12);
